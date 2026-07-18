"""
Скрипт переобучения ML-моделей для ml_score.py.

Запуск (из корня проекта):
    python train_ml.py [--db history.db] [--out analysis/ml]

Фильтрация датасета:
    - Только строки с outcome IN ('bounce', 'breakout')  ← partial исключён
    - Исключаем strength_claude = 0 (уровни без Claude-оценки, bounce 18% vs 40%+)
    - Исключаем записи до 2026-05-21 (аномальный медвежий режим, breakout 35% vs 14-17%)
    - Исключаем touches_count = 0 (артефакты записи, 0% bounce, n=4 — не рыночный исход)

  ВАЖНО: partial — это артефакт прерванного мониторинга (duration=0), а не
  рыночный исход. Включение partial в обучение ухудшает качество модели, так как
  она начинает предсказывать его как легитимный класс и занижает p_bounce без причины.

  Hard-filter по touches делается в apply_ml_to_level() ДО вызова ML:
    touches >= 2 → 0% bounce → блокируем без ML.
  touches УБРАН из признаков модели — он доминировал (importance 0.542) и делал
  классификатор тривиальным. Вся логика touches закрыта хард-фильтром.

  Строки с touches >= 2 ВКЛЮЧЕНЫ в обучение как валидные breakout-метки (+445 строк, ~20% breakout).
  Это не противоречие: модель учится на всех исходах, но touches не является признаком.
  На инференсе хард-фильтр срабатывает ДО ML — модель при touches>=2 не вызывается.

Признаки (6 штук, порядок должен совпадать с ml_score.py):  # FIX BUG-5: monitoring_age_hours удалён
    1. strength_claude  — сила уровня (1-5)
    2. ltype_enc        — тип уровня (pump_base / body_level / wick_level / order_block)
    3. vol_capped       — vol_ratio_at_touch, обрезан до 20
    4. atr_capped       — atr_ratio, обрезан до 20
    5. style_enc        — approach_style (flash=0, impulse=1, bleed=2, unknown=3)
    6. age_capped       — monitoring_age_minutes, обрезан до 300

Модели:
    clf  — RandomForestClassifier (bounce/breakout при touches=1)
    clf2 — RandomForestClassifier (быстрый breakout: touches≤3 vs медленный: touches>10)
           обучается только на breakout-записях с touches >= 1
           Полезен для решения "продолжать мониторить или нет"
    reg  — RandomForestRegressor (fill_depth_pct, улучшен: обучается раздельно
           по bounce и breakout, предсказывает на смеси)

Выходные файлы (перезаписывают существующие):
    analysis/ml/clf.pkl            — RandomForest классификатор (bounce/breakout)
    analysis/ml/clf2.pkl           — классификатор скорости breakout (fast/slow)
    analysis/ml/reg.pkl            — RandomForest регрессор (fill_depth_pct)
    analysis/ml/label_encoder.pkl  — LabelEncoder классов clf
    analysis/ml/level_type_map.pkl — маппинг level_type → int
    analysis/ml/thresholds.json    — пороги p_bounce для ml_delta
"""

import argparse
import os
import pickle
import sqlite3

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import (brier_score_loss, classification_report,
                             mean_absolute_error)
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.preprocessing import LabelEncoder

# Импортируем константу STYLE_MAP из ml_score, чтобы не дублировать
import sys
sys.path.insert(0, os.path.dirname(__file__))
from analysis.ml_score import STYLE_MAP

# touches убран: доминировал (importance 0.542), тривиализировал модель.
# Логика touches полностью закрыта хард-фильтром в apply_ml_to_level().
# FIX BUG-5: monitoring_age_hours удалён — дублировал age_capped, при обучении
# отсутствовал у 95% записей (= 0) → train/inference skew.
# age_capped удалён: >75% записей имеют age=0 (поле заполняется только при
# завершении монитора — сбор нарушен). Мёртвый признак добавлял шум и мешал
# калибровке. При восстановлении сбора — вернуть в список.
FEATURES = ["strength_claude", "ltype_enc", "vol_capped", "atr_capped", "style_enc"]


def load_data(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        """
        SELECT *
        FROM level_outcomes
        WHERE outcome IN ('bounce', 'breakout')
          AND strength_claude != 0
          AND created_at >= '2026-05-21'
          AND touches_count >= 1
          -- touches_count >= 2 включены: 445 реальных breakout-меток которые модель не видела.
          -- touches убран из признаков (был dominant feature, importance=0.542).
          -- Хард-фильтр в apply_ml_to_level закрывает touches>=2 на инференсе — обучение не ломает логику.
        """,
        conn,
    )
    conn.close()

    print(f"Строк после фильтрации: {len(df)}")
    if len(df) < 50:
        raise ValueError(f"Слишком мало данных для обучения: {len(df)} строк")

    print("Распределение исходов:")
    print(df["outcome"].value_counts(normalize=True).mul(100).round(1).to_string())
    print()

    # Диагностика: touches >= 2 = 0% bounce (подтверждаем хард-фильтр)
    print("bounce% по touches_count (первые 6 значений):")
    for tc, grp in list(df.groupby("touches_count"))[:6]:
        b = round((grp["outcome"] == "bounce").mean() * 100, 1)
        print(f"  touches={int(tc)}  bounce={b}%  n={len(grp)}")
    print()

    # Диагностика: NULL vol_ratio_at_touch у breakout при touches=1
    t1_break = df[(df["touches_count"] == 1) & (df["outcome"] == "breakout")]
    null_vol = t1_break["vol_ratio_at_touch"].isna().sum()
    if null_vol > 0:
        print(f"  ⚠️  vol_ratio_at_touch=NULL у {null_vol}/{len(t1_break)} breakout при touches=1 "
              f"— главный сигнал breakout теряется. Починить запись в monitor.py.")
    print()

    return df


def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Строит матрицу признаков, возвращает (X, level_type_map)."""
    level_type_map = {t: i for i, t in enumerate(sorted(df["level_type"].dropna().unique()))}

    df = df.copy()
    df["ltype_enc"]  = df["level_type"].map(level_type_map).fillna(1).astype(int)
    df["style_enc"]  = (
        df["approach_style"]
        .fillna("unknown")
        .map(STYLE_MAP)
        .fillna(STYLE_MAP["unknown"])
        .astype(int)
    )
    df["vol_capped"] = df["vol_ratio_at_touch"].clip(upper=20).fillna(1.0)
    df["atr_capped"] = df["atr_ratio"].clip(upper=20).fillna(1.0)
    # MEDIUM-5 fix: monitoring_age_minutes теперь ВСЕГДА хранит секунды —
    # все писатели (monitor.py:_get_market_context, monitor.py breakout re-score,
    # main.py:_monitored duration) унифицированы. Старая эвристика "≤300 = минуты,
    # >300 = секунды" угадывала формат постфактум и расходилась с инференсом
    # (ml_score.py всегда делил /60, считая поле секундами) — убрана.
    # age_capped удалён из признаков (см. FEATURES). Вычисление оставлено для
    # диагностики заполняемости поля, но в X не включается.
    age_raw = df["monitoring_age_minutes"].fillna(0).astype(float)
    age_in_minutes = age_raw / 60
    age_nonzero_pct = (age_in_minutes > 0).mean() * 100
    print(f"  [диагноз] monitoring_age: ненулевые {age_nonzero_pct:.1f}% записей "
          f"({'норм.' if age_nonzero_pct >= 30 else '⚠️  мало — признак не включён'})")

    X = df[FEATURES].copy()

    print("Статистика признаков:")
    print(X.describe().round(2).to_string())
    print()

    # bounce% по approach_style
    print("bounce% по approach_style:")
    style_bounce = df.groupby("approach_style")["outcome"].apply(
        lambda s: (s == "bounce").mean()
    )
    counts = df["approach_style"].value_counts()
    for style in style_bounce.index:
        print(f"  {style:<10} bounce={style_bounce[style]:.1%}  n={counts.get(style, 0)}")
    print()

    # bounce% по level_type
    print("bounce% по level_type:")
    for lt, grp in df.groupby("level_type"):
        b = round((grp["outcome"] == "bounce").mean() * 100, 1)
        print(f"  {lt:<15} bounce={b}%  n={len(grp)}")
    print()

    return X, level_type_map


def train(db_path: str, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)

    df = load_data(db_path)
    X, level_type_map = build_features(df)

    # ── Классификатор bounce/breakout ──────────────────────────────────
    le = LabelEncoder()
    y_clf = le.fit_transform(df["outcome"])

    print(f"Классы: {list(le.classes_)}")
    print()

    # Базовый RF — class_weight=balanced устраняет дисбаланс классов.
    # oob_score оставляем для диагностики, но для калибровки не используем
    # (isotonic требует отдельный hold-out — делаем через cross_val).
    _rf_base = RandomForestClassifier(
        n_estimators=200,
        max_depth=6,
        min_samples_leaf=5,
        random_state=42,
        class_weight="balanced",
        oob_score=True,
        n_jobs=-1,
    )

    # C: Калибровка isotonic через кросс-валидацию (cv=5) устраняет инверсию
    # p_bounce на верхнем конце (0.95→WR49%), которую показал health-отчёт.
    # После калибровки p_bounce ≥ THRESHOLD_HIGH должен соответствовать реальной
    # доле bounce, а не быть завышен RF из-за знакомых паттернов на трейне.
    # Важно: CalibratedClassifierCV(cv=5) обучает 5 базовых моделей + calibrator
    # — итоговый объект predict_proba использует усреднение, формат выхода
    # для ml_score.py (proba[bounce_idx]) сохраняется без изменений.
    clf = CalibratedClassifierCV(_rf_base, method="isotonic", cv=5)
    clf.fit(X, y_clf)

    bounce_idx = list(le.classes_).index("bounce")

    # Диагностика после калибровки
    proba_cal = clf.predict_proba(X)[:, bounce_idx]
    brier = brier_score_loss((y_clf == bounce_idx).astype(int), proba_cal)
    print(f"Brier score (после калибровки): {brier:.4f}  "
          f"({'✅ лучше монетки' if brier < 0.25 else '⚠️  хуже монетки 0.25'})")

    # Проверка монотонности: p_bounce должен расти с реальной долей отбоев по корзинам
    bins = [0.0, 0.50, 0.70, 0.85, 0.95, 1.01]
    bin_labels = ["0.00–0.50", "0.50–0.70", "0.70–0.85", "0.85–0.95", "0.95–1.00"]
    y_true_binary = (y_clf == bounce_idx).astype(int)
    prev_wr = -1.0
    monotone = True
    print("Калибровка по корзинам p_bounce (после isotonic):")
    for i, label in enumerate(bin_labels):
        mask = (proba_cal >= bins[i]) & (proba_cal < bins[i+1])
        n_bin = mask.sum()
        if n_bin == 0:
            print(f"  {label}  n=0  —")
            continue
        wr = y_true_binary[mask].mean()
        flag = ""
        if wr < prev_wr - 0.03:
            flag = "  ⚠️  немонотонность"
            monotone = False
        prev_wr = wr
        print(f"  {label}  n={n_bin:4d}  WR={wr:.1%}  avg_p={proba_cal[mask].mean():.3f}{flag}")
    if monotone:
        print("  ✅ Монотонность соблюдена")
    else:
        print("  ⚠️  Нарушение монотонности — попробуй больше данных или 'sigmoid' вместо 'isotonic'")
    print()

    cv_scores = cross_val_score(clf, X, y_clf, cv=5, scoring="f1_macro")
    print(f"CV F1-macro: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
    print()

    print("Классификация на трейне:")
    print(classification_report(y_clf, clf.predict(X), target_names=le.classes_))

    # CalibratedClassifierCV не имеет feature_importances_ напрямую —
    # берём усреднение из базовых estimators внутри cv-fold'ов.
    try:
        import numpy as _np
        _importances = _np.mean(
            [est.estimator.feature_importances_
             for est in clf.calibrated_classifiers_], axis=0
        )
    except Exception:
        _importances = [0.0] * len(FEATURES)
    print("Важность признаков (классификатор bounce/breakout, усреднение по folds):")
    for feat, imp in sorted(
        zip(FEATURES, _importances), key=lambda x: -x[1]
    ):
        print(f"  {feat:<20} {imp:.3f}")
    print()

    # ── Калибровка порогов ────────────────────────────────────────────
    # Пороги считаем по OOB-предсказаниям (out-of-bag), а не по трейну.
    # RandomForest на трейне даёт p_bounce близко к 1.0 для знакомых записей,
    # поэтому P75 на трейне улетает к 0.999 и ml_delta=+1 становится недостижимым.
    # OOB: каждое дерево предсказывает только те записи, на которых не обучалось —
    # честная оценка без переобучения, без потери обучающих данных.
    # CalibratedClassifierCV не имеет oob_decision_function_ — используем
    # predict_proba на трейне. После isotonic-калибровки распределение сжимается
    # к честным вероятностям, поэтому P25/P75 будут ниже, чем у сырого RF.
    # Это ожидаемо и правильно: пороги теперь отражают реальную долю bounce.
    all_proba = clf.predict_proba(X)[:, bounce_idx]
    p25 = np.percentile(all_proba, 25)
    p75 = np.percentile(all_proba, 75)
    print(f"p_bounce percentiles (train, post-calibration): P25={p25:.3f}  P75={p75:.3f}")
    print(f"  → пороги для ml_score.py: THRESHOLD_HIGH={p75:.2f}  THRESHOLD_LOW={p25:.2f}")
    if not (0.50 <= p75 <= 0.95):
        print(f"  ⚠️  P75={p75:.3f} за пределами ожидаемого диапазона 0.50–0.95")
    print()

    # ── Классификатор скорости breakout (clf2) ────────────────────────
    # Задача: предсказать, будет ли breakout быстрым (touches≤3) или медленным (touches>10).
    # Обучается только на breakout-записях. Полезен для решения "продолжать мониторить или нет":
    # если clf2 предсказывает fast → уровень слабый, можно не тратить ресурсы на долгий монитор.
    df_break = df[df["outcome"] == "breakout"].copy()
    clf2 = None
    if len(df_break) >= 30:
        # Метки: fast = touches ≤ 3, slow = touches > 10; средние (4-10) исключаем как шум
        mask_break2 = (
            ((df["outcome"] == "breakout") & (df["touches_count"] <= 3)) |
            ((df["outcome"] == "breakout") & (df["touches_count"] > 10))
        )
        df_break2 = df[mask_break2].copy()
        df_break2["speed"] = (df_break2["touches_count"] <= 3).astype(int)  # 1=fast, 0=slow
        X2 = X.loc[df_break2.index]
        y2 = df_break2["speed"]

        if y2.nunique() == 2 and len(df_break2) >= 20:
            clf2 = RandomForestClassifier(
                n_estimators=200,
                max_depth=4,
                min_samples_leaf=3,
                random_state=42,
                class_weight="balanced",
                n_jobs=-1,
            )
            clf2.fit(X2, y2)
            cv2 = cross_val_score(clf2, X2, y2, cv=min(5, len(df_break2) // 4), scoring="f1_macro")
            print(f"clf2 (fast/slow breakout): CV F1-macro={cv2.mean():.3f} ± {cv2.std():.3f}")
            print(f"  fast(≤3 touches): n={y2.sum()}  slow(>10 touches): n={(y2==0).sum()}")
            print("  Важность признаков (clf2):")
            for feat, imp in sorted(zip(FEATURES, clf2.feature_importances_), key=lambda x: -x[1]):
                print(f"    {feat:<20} {imp:.3f}")
            print()
        else:
            print(f"clf2: недостаточно данных (fast={y2.sum()}, slow={(y2==0).sum()}, n={len(df_break2)}), пропуск")
            print()
    else:
        print(f"clf2: слишком мало breakout-записей ({len(df_break)}), пропуск")
        print()

    # ── Регрессор fill_depth_pct ──────────────────────────────────────
    # Улучшение: обучаем на всех записях (bounce + breakout), но добавляем флаг is_breakout
    # как дополнительный признак — модель учится, что у breakout глубина системно выше.
    # fill_depth для bounce: медиана 0.34%, max 7.25%
    # fill_depth для breakout: медиана 2.17%, max 18.7%
    X_reg = X.copy()
    X_reg["is_breakout"] = (df["outcome"] == "breakout").astype(float).values
    y_reg = df["fill_depth_pct"].fillna(0).clip(lower=0)

    reg = RandomForestRegressor(
        n_estimators=200,
        max_depth=8,          # глубже, чем clf — регрессия требует точности
        min_samples_leaf=3,
        random_state=42,
        n_jobs=-1,
    )
    reg.fit(X_reg, y_reg)
    preds = reg.predict(X_reg)
    mae = mean_absolute_error(y_reg, preds)
    print(f"Регрессор fill_depth MAE на трейне: {mae:.3f}%")

    # MAE раздельно по bounce и breakout
    mask_b = df["outcome"] == "bounce"
    print(f"  bounce   MAE: {mean_absolute_error(y_reg[mask_b], preds[mask_b]):.3f}%  "
          f"(медиана actual={y_reg[mask_b].median():.2f}%)")
    print(f"  breakout MAE: {mean_absolute_error(y_reg[~mask_b], preds[~mask_b]):.3f}%  "
          f"(медиана actual={y_reg[~mask_b].median():.2f}%)")

    print("  Важность признаков (регрессор):")
    reg_features = FEATURES + ["is_breakout"]
    for feat, imp in sorted(zip(reg_features, reg.feature_importances_), key=lambda x: -x[1]):
        print(f"    {feat:<20} {imp:.3f}")
    print()

    # ── Smoke-тесты ────────────────────────────────────────────────────
    body_enc = level_type_map.get("body_level", 0)
    pump_enc = level_type_map.get("pump_base", 1)

    # Smoke-тесты: age_capped убран из FEATURES → тесты теперь 5 признаков
    print("Smoke-тест 1: body_level (strength=4, vol=1.5, atr=2.0):")
    for style_name, style_code in STYLE_MAP.items():
        x_test = pd.DataFrame(
            [[4, body_enc, 1.5, 2.0, style_code]],
            columns=FEATURES,
        )
        proba = clf.predict_proba(x_test)[0]
        p_b   = proba[bounce_idx]
        delta = "+1" if p_b >= p75 else ("-1" if p_b <= p25 else " 0")
        print(f"  {style_name:<10} p_bounce={p_b:.3f}  ml_delta={delta}")
    print()

    print("Smoke-тест 2: pump_base (strength=5, vol=1.5, atr=2.0):")
    for style_name, style_code in STYLE_MAP.items():
        x_test = pd.DataFrame(
            [[5, pump_enc, 1.5, 2.0, style_code]],
            columns=FEATURES,
        )
        proba = clf.predict_proba(x_test)[0]
        p_b   = proba[bounce_idx]
        delta = "+1" if p_b >= p75 else ("-1" if p_b <= p25 else " 0")
        print(f"  {style_name:<10} p_bounce={p_b:.3f}  ml_delta={delta}")
    print()

    print("Smoke-тест 3: vol высокий (×5) vs низкий (body_level, bleed):")
    for vol, label in [(0.8, "vol=0.8 (тихий) "), (5.0, "vol=5.0 (спайк)  ")]:
        x_test = pd.DataFrame(
            [[4, body_enc, vol, 2.0, STYLE_MAP["bleed"]]],
            columns=FEATURES,
        )
        proba = clf.predict_proba(x_test)[0]
        p_b   = proba[bounce_idx]
        delta = "+1" if p_b >= p75 else ("-1" if p_b <= p25 else " 0")
        print(f"  {label}  p_bounce={p_b:.3f}  ml_delta={delta}")
    print()

    # ── Сохранение ─────────────────────────────────────────────────────
    pickle.dump(clf,            open(os.path.join(out_dir, "clf.pkl"),            "wb"))
    pickle.dump(reg,            open(os.path.join(out_dir, "reg.pkl"),            "wb"))
    pickle.dump(le,             open(os.path.join(out_dir, "label_encoder.pkl"),  "wb"))
    pickle.dump(level_type_map, open(os.path.join(out_dir, "level_type_map.pkl"), "wb"))
    if clf2 is not None:
        pickle.dump(clf2, open(os.path.join(out_dir, "clf2.pkl"), "wb"))
        print(f"   clf2.pkl сохранён")

    import json as _json
    thresholds = {"THRESHOLD_HIGH": round(float(p75), 4), "THRESHOLD_LOW": round(float(p25), 4)}
    with open(os.path.join(out_dir, "thresholds.json"), "w") as _f:
        _json.dump(thresholds, _f, indent=2)

    print(f"✅ Модели сохранены в {out_dir}/")
    print(f"   Классы: {list(le.classes_)}")
    print(f"   level_type_map: {level_type_map}")
    print(f"   Пороги: THRESHOLD_HIGH={p75:.4f}  THRESHOLD_LOW={p25:.4f}")
    print()

    return len(df)


# ══════════════════════════════════════════════════════════════════════
# ЭКСПЕРИМЕНТ A: модель на целевой «полный фил vs частичный» из live_trades
# ══════════════════════════════════════════════════════════════════════
#
# Мотивация (из анализа сессии 28-30.06.2026):
#   В health-отчёте clf (bounce/breakout) MAE=0.54 на стратегии bounce —
#   хуже монетки. Целевая «отбой зоны» слабо связана с PnL: full-fill +
#   возврат = «bounce» по level_outcomes, но убыток по live_trades.
#   Пять проверок (V1/V5/V7/рынок/время) показали: исход сделки S2 не
#   предсказывается существующими признаками. Это НЕ значит, что ML
#   бесполезен — но нужно честно измерить потолок на правильной целевой.
#
# Что делает эта функция:
#   Обучает ОТДЕЛЬНЫЙ экспериментальный классификатор на бинарной целевой
#   «полный грид (fill_count>=10) vs частичный» из live_trades.db.
#   Если AUC/Brier значимо лучше монетки — у нас впервые предиктор full-fill.
#   Если нет — честно закрываем гипотезу с числами.
#
# Важно:
#   1. Эта модель НЕ заменяет боевой clf. Результат — только в stdout и
#      опциональный fullgrid_clf.pkl (не читается ml_score.py).
#   2. Загружает live_trades.db ОТДЕЛЬНО от history.db (разные БД).
#   3. Запускается только через --experiment-fullgrid.
#
def _load_live_trades_flexible(source: str) -> pd.DataFrame:
    """Загрузить сделки из источника, устойчиво к разнице схем между днями.

    source может быть:
      - путь к одному .db файлу (напр. live_trades.db с историей);
      - путь к папке (напр. live_trades/) — тогда склеиваются ВСЕ live_trades_*.db.

    Колонки, которых нет в конкретном файле (ob_*/oi_* в старых днях), берутся
    как NULL, а не роняют запрос — читаем только реально существующие столбцы
    и дополняем недостающие.
    """
    import glob as _glob

    wanted = [
        "level_type", "grid_fill_count", "vol_ratio_at_entry", "strength_at_entry",
        "p_bounce_at_entry", "approach_count_at_entry", "ob_bid_vol_grid",
        "ob_imbalance", "ob_spread_pct", "funding_rate_at_entry", "oi_change_1h_pct",
        "pnl_usdt", "exit_reason",
    ]

    if os.path.isdir(source):
        files = sorted(_glob.glob(os.path.join(source, "live_trades_*.db")))
    else:
        files = [source]

    frames = []
    for f in files:
        try:
            conn = sqlite3.connect(f)
            have = {r[1] for r in conn.execute("PRAGMA table_info(live_trades)")}
            cols = [c for c in wanted if c in have]
            if "grid_fill_count" not in cols or "pnl_usdt" not in cols:
                conn.close()
                continue
            part = pd.read_sql_query(
                f"SELECT {', '.join(cols)} FROM live_trades "
                f"WHERE pnl_usdt IS NOT NULL AND grid_fill_count > 0 "
                f"AND exit_reason NOT IN "
                f"('reconcile_no_fill','timeout_no_fill','cancelled_no_fill')",
                conn,
            )
            conn.close()
            # добить недостающие колонки как NaN
            for c in wanted:
                if c not in part.columns:
                    part[c] = np.nan
            frames.append(part[wanted])
        except Exception as e:
            print(f"  [WARN] {f}: {e}")
            continue

    if not frames:
        return pd.DataFrame(columns=wanted)
    return pd.concat(frames, ignore_index=True)


def train_fullgrid_model(live_db_path: str, out_dir: str) -> None:
    """Эксперимент A: предсказать full-fill (grid_fill_count>=10) из live_trades."""
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import StratifiedKFold

    print("=" * 60)
    print("ЭКСПЕРИМЕНТ A: full-fill vs partial (live_trades)")
    print(f"Источник: {live_db_path}")
    print("=" * 60)

    # Порог надёжности выборки: ниже него вывод об AUC статистически
    # неустойчив (±0.15-0.20 разброса) и не может «доказать» ни предсказуемость,
    # ни её отсутствие. Основано на наблюдении, что n=45 давал AUC=0.47±0.17.
    MIN_RELIABLE_N = 100

    # ── Загрузка данных (устойчиво к разнице схем и папке/файлу) ──────
    df = _load_live_trades_flexible(live_db_path)
    if df.empty:
        print("[ОШИБКА] Нет данных ни в одном источнике.")
        return

    n_total = len(df)
    print(f"Всего сделок: {n_total}")
    if n_total < 50:
        print(f"⚠️  Мало данных (n={n_total} < 50) — результаты ненадёжны.")

    # ── Целевая переменная ─────────────────────────────────────────────
    FULL_FILL_THRESHOLD = 10
    df["is_full_fill"] = (df["grid_fill_count"] >= FULL_FILL_THRESHOLD).astype(int)
    n_full = df["is_full_fill"].sum()
    n_part = n_total - n_full
    print(f"Полный фил (fill>={FULL_FILL_THRESHOLD}): {n_full} ({n_full/n_total:.1%})")
    print(f"Частичный:                                 {n_part} ({n_part/n_total:.1%})")
    print()

    # ── Признаки: сначала только те, что гарантированно заполнены ─────
    BASE_FEATURES = ["vol_ratio_at_entry", "strength_at_entry",
                     "p_bounce_at_entry", "approach_count_at_entry"]
    NEW_FEATURES  = ["ob_bid_vol_grid", "ob_imbalance", "ob_spread_pct",
                     "funding_rate_at_entry", "oi_change_1h_pct"]

    def run_experiment(feat_list: list, label: str) -> tuple[float | None, int]:
        """Обучить на feat_list, вернуть (AUC, n_после_dropna). AUC=None если мало данных."""
        available = [f for f in feat_list if f in df.columns]
        dfx = df[available + ["is_full_fill"]].dropna()
        n = len(dfx)
        if n < 30:
            print(f"  [{label}] n={n} после dropna — слишком мало, пропуск")
            return None, n
        X_e = dfx[available]
        y_e = dfx["is_full_fill"]
        n_pos = y_e.sum()
        if n_pos < 5 or (n - n_pos) < 5:
            print(f"  [{label}] недостаточно примеров одного класса — пропуск")
            return None, n

        clf_e = CalibratedClassifierCV(
            RandomForestClassifier(
                n_estimators=200, max_depth=5, min_samples_leaf=5,
                class_weight="balanced", random_state=42, n_jobs=-1
            ),
            method="isotonic", cv=min(5, n_pos),
        )

        # Стратифицированный CV для оценки AUC
        skf = StratifiedKFold(n_splits=min(5, n_pos), shuffle=True, random_state=42)
        aucs = []
        for train_idx, val_idx in skf.split(X_e, y_e):
            clf_e.fit(X_e.iloc[train_idx], y_e.iloc[train_idx])
            p = clf_e.predict_proba(X_e.iloc[val_idx])[:, 1]
            try:
                aucs.append(roc_auc_score(y_e.iloc[val_idx], p))
            except ValueError:
                pass  # один класс в fold

        auc_mean = float(np.mean(aucs)) if aucs else 0.5
        auc_std  = float(np.std(aucs)) if aucs else 0.0
        brier = brier_score_loss(y_e, clf_e.predict_proba(X_e)[:, 1])

        flag = "✅ лучше случайного" if auc_mean > 0.55 else "⚠️  не лучше монетки"
        warn_n = "  ⚠️  n мал — большой разброс, вывод ненадёжен" if n < MIN_RELIABLE_N else ""
        print(f"  [{label}] n={n}  AUC={auc_mean:.3f}±{auc_std:.3f}  "
              f"Brier={brier:.3f}  {flag}{warn_n}")
        print(f"    Признаки: {available}")

        # Важность признаков (из последнего fold)
        try:
            _imp = np.mean([
                est.estimator.feature_importances_
                for est in clf_e.calibrated_classifiers_
            ], axis=0)
            for f, i in sorted(zip(available, _imp), key=lambda x: -x[1]):
                print(f"      {f:<30} {i:.3f}")
        except Exception:
            pass

        return auc_mean, n

    # Прогон 1: только базовые признаки (всегда заполнены)
    print("── Прогон 1: базовые признаки (vol_ratio/strength/p_bounce/approach) ──")
    auc_base, n_base = run_experiment(BASE_FEATURES, "base")
    print()

    # Прогон 2: базовые + новые L2/OI (только там, где не NULL)
    auc_new, n_new = None, 0
    new_available = [f for f in NEW_FEATURES
                     if f in df.columns and df[f].notna().sum() >= 20]
    if new_available:
        print(f"── Прогон 2: +L2/OI признаки ({new_available}) ──")
        auc_new, n_new = run_experiment(BASE_FEATURES + new_available, "base+L2/OI")
        print()
        if auc_new and auc_base and auc_new > auc_base + 0.02:
            print(f"  ✅ Новые признаки улучшают AUC: {auc_base:.3f} → {auc_new:.3f} (+{auc_new-auc_base:.3f})")
        else:
            print(f"  — Новые признаки не улучшают AUC значимо (delta<0.02 или нет данных)")
    else:
        print(f"── Прогон 2: L2/OI признаки ещё не накоплены (всё NULL), пропуск ──")
        print(f"   Копится форвардно. Запусти снова через 1-2 недели.")
    print()

    # ── Итоговый вывод ─────────────────────────────────────────────────
    # ВАЖНО: не выдавать уверенный вердикт «непредсказуемо» на малой выборке.
    # dropna по at-entry полям (vol_ratio/p_bounce/approach) может схлопнуть
    # выборку втрое (старые сделки без этих полей), и тогда AUC±std огромен.
    print("── ВЫВОД ЭКСПЕРИМЕНТА A ──────────────────────────────────────")
    if auc_base is None:
        print(f"  Недостаточно данных для вывода (n_base={n_base}).")
    elif n_base < MIN_RELIABLE_N:
        print(f"  ВЫБОРКА МАЛА (n={n_base} < {MIN_RELIABLE_N}) — вывод ПРЕЖДЕВРЕМЕНЕН.")
        print(f"  AUC={auc_base:.3f} на таком n имеет разброс ±0.15-0.20, то есть")
        print(f"  статистически неотличим от монетки в ОБЕ стороны. Это НЕ доказательство")
        print(f"  ни предсказуемости, ни непредсказуемости — просто мало заполненных")
        print(f"  at-entry полей (старые сделки писались без vol_ratio/p_bounce/approach).")
        print(f"  Дать боту накопить >= {MIN_RELIABLE_N} сделок с полными полями и повторить.")
    elif auc_base > 0.55:
        print(f"  AUC={auc_base:.3f} > 0.55 на надёжной выборке (n={n_base}).")
        print(f"  Предиктор full-fill ЕСТЬ. Рассмотреть интеграцию в ml_score.py")
        print(f"  ('p_full_fill' → штраф ml_delta для сетапов с высоким риском полного фила).")
    else:
        print(f"  AUC={auc_base:.3f} ≤ 0.55 на надёжной выборке (n={n_base}).")
        print(f"  Предиктор full-fill НЕ работает на базовых признаках — это надёжный")
        print(f"  вывод, согласующийся с V1/V5/V7: исход входа S2 непредсказуем по")
        print(f"  существующим признакам. Единственная надежда — L2/OI (прогон 2),")
        print(f"  когда накопятся. Если и они не дадут AUC>0.55 — гипотеза закрыта.")
    print("=" * 60)
    print()


RETRAIN_THRESHOLD = 200   # переобучать каждые N новых записей
_TRAIN_SIZE_FILE  = "analysis/ml/last_train_size.txt"


def get_last_train_size() -> int:
    """Читает количество записей на момент последнего обучения."""
    try:
        with open(_TRAIN_SIZE_FILE) as f:
            return int(f.read().strip())
    except Exception:
        return 0


def save_train_size(n: int) -> None:
    os.makedirs(os.path.dirname(_TRAIN_SIZE_FILE), exist_ok=True)
    with open(_TRAIN_SIZE_FILE, "w") as f:
        f.write(str(n))


def should_retrain(db_path: str) -> bool:
    """Возвращает True, если накопилось >= RETRAIN_THRESHOLD новых записей."""
    try:
        conn = sqlite3.connect(db_path)
        cur  = conn.execute(
            "SELECT COUNT(*) FROM level_outcomes "
            "WHERE outcome IN ('bounce','breakout') AND strength_claude != 0"
        )
        current = cur.fetchone()[0]
        conn.close()
        last = get_last_train_size()
        return (current - last) >= RETRAIN_THRESHOLD
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Переобучение ML-моделей для ml_score.py")
    parser.add_argument("--db",    default="history.db",  help="Путь к history.db")
    parser.add_argument("--out",   default="analysis/ml", help="Папка для .pkl файлов")
    parser.add_argument("--force", action="store_true",   help="Переобучить без проверки порога")
    parser.add_argument("--experiment-fullgrid", action="store_true",
                        help="[Эксперимент A] Обучить модель full-fill vs partial из live_trades.db "
                             "(не заменяет боевую модель, только диагностика)")
    parser.add_argument("--live-db", default="live_trades.db",
                        help="Источник для --experiment-fullgrid: путь к .db файлу "
                             "(напр. live_trades.db с историей) ИЛИ к папке "
                             "(напр. live_trades/ — склеит все дневные файлы). "
                             "Устойчив к разнице схем: ob/oi-колонки берутся как NULL, "
                             "где их нет.")
    args = parser.parse_args()

    # Эксперимент A — запускается независимо от боевого обучения
    if args.experiment_fullgrid:
        train_fullgrid_model(args.live_db, args.out)
        # Если только эксперимент — не продолжать боевое обучение
        # (можно комбинировать: python train_ml.py --force --experiment-fullgrid)
        if not args.force and not should_retrain(args.db):
            return

    if not args.force and not should_retrain(args.db):
        last = get_last_train_size()
        conn = sqlite3.connect(args.db)
        cur  = conn.execute(
            "SELECT COUNT(*) FROM level_outcomes "
            "WHERE outcome IN ('bounce','breakout') AND strength_claude != 0"
        )
        current = cur.fetchone()[0]
        conn.close()
        print(f"Пропуск: новых записей {current - last} < {RETRAIN_THRESHOLD}. "
              f"Используйте --force для принудительного переобучения.")
        return

    print(f"DB: {args.db}")
    print(f"Out: {args.out}")
    print()
    n = train(args.db, args.out)
    if n:
        save_train_size(n)


if __name__ == "__main__":
    main()
