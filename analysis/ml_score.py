"""
ML scoring for support levels.

Два выхода основного классификатора:
  - p_bounce: float 0..1  — вероятность отбоя
  - expected_depth: float — ожидаемый прокол под уровень в %

Дополнительный выход clf2 (только при ml_blocked=False):
  - p_fast_breakout: float 0..1 — вероятность быстрого пробоя (touches≤3)
    Полезен для решения "продолжать мониторить или нет"

Использование:
    from analysis.ml_score import ml_score, apply_ml_to_level

    # approach_style должен быть выставлен ДО вызова (из detect_approach_style):
    lvl["approach_style"] = detect_approach_style(symbol)

    calculate_strength(lvl)
    apply_ml_to_level(lvl)
    # lvl теперь содержит: p_bounce, expected_depth, ml_delta, strength_pre_ml,
    #                       p_fast_breakout (если clf2 загружен)

Признаки модели (5 штук, touches и age_capped убраны):
    1. strength        — Python-сила уровня (1-5)
    2. ltype_enc       — тип уровня (из level_type_map.pkl)
    3. vol_ratio       — объём / среднее (обрезается до 20)
    4. atr_ratio       — расстояние до уровня в ATR (обрезается до 20)
    5. style_enc       — стиль подхода: flash=0, impulse=1, bleed=2, unknown=3
                         Доступен только в _monitored(); в _run_phase1 = "unknown"

  age_capped убран (health-отчёт: >75% записей age=0, мёртвый признак,
  синхронно с train_ml.py). touches убран ранее: доминировал и делал модель
  тривиальной.

Hard-filter СНЯТ (25.06): ранее touches >= 2 → ml_delta = -2, p_bounce = 0.0.
    Снят, т.к. по стратегии G2 (2й подход) и G4 (флип-ретест) должны торговаться.
    Блок 3+ подходов (G3) теперь в strategy2_signal_filter (гейт по approach_count).
    ML отдаёт сырой p_bounce по 6 признакам.

Регрессор fill_depth:
    Обучен на bounce + breakout с признаком is_breakout.
    При инференсе is_breakout=0 (мы ещё не знаем исход — прогнозируем ожидаемую глубину
    при отбое). Это консервативная оценка; реальная глубина при пробое выше.
"""

from __future__ import annotations
import asyncio
import math
import os
import pickle
import numpy as np

from logger import logger

# Paths — модели лежат рядом с этим файлом в папке ml/
_BASE = os.path.join(os.path.dirname(__file__), "ml")

_clf = None
_clf2 = None   # классификатор скорости breakout (fast/slow), опциональный
_reg = None
_le  = None
_type_map = None

_model_lock: asyncio.Lock | None = None


def _get_lock() -> asyncio.Lock:
    global _model_lock
    if _model_lock is None:
        _model_lock = asyncio.Lock()
    return _model_lock

# Маппинг стилей подхода — константа, используется и при обучении, и при инференсе
STYLE_MAP: dict[str, int] = {
    "flash":   0,   # резкий удар ×2 объём → часто sweep перед разворотом → bounce вероятнее
    "impulse": 1,   # 3+ зелёных → красная → первый серьёзный тест уровня → нейтрально
    "bleed":   2,   # 4+ красных с растущим объёмом → методичное давление → bounce реже
    "unknown": 3,   # стиль не определён (триггерный путь, старт)
}

# Пороги p_bounce для ml_delta.
# Откалиброваны по P25/P75 на датасете outcome IN ('bounce','breakout'),
# strength!=0, created_at >= 2026-05-21 (partial исключён).
# Обновляются автоматически из thresholds.json после каждого переобучения.
THRESHOLD_HIGH: float = 0.97   # p_bounce >= HIGH → ml_delta = +1
THRESHOLD_LOW:  float = 0.60   # p_bounce <= LOW  → ml_delta = -1

# DEPRECATED (25.06): хард-блок touches>=TOUCHES_BLOCK СНЯТ — G2/G4 должны торговаться.
# Константа сохранена, чтобы не ломать возможные внешние импорты; в логике не используется.
# Блок 3+ подходов (G3) перенесён в strategy2_signal_filter (гейт по approach_count).
TOUCHES_BLOCK: int = 2


def _load() -> bool:
    global _clf, _clf2, _reg, _le, _type_map, THRESHOLD_HIGH, THRESHOLD_LOW
    if _clf is not None:
        return True
    try:
        with open(os.path.join(_BASE, "clf.pkl"), "rb") as f:
            _clf = pickle.load(f)
        with open(os.path.join(_BASE, "reg.pkl"), "rb") as f:
            _reg = pickle.load(f)
        with open(os.path.join(_BASE, "label_encoder.pkl"), "rb") as f:
            _le = pickle.load(f)
        with open(os.path.join(_BASE, "level_type_map.pkl"), "rb") as f:
            _type_map = pickle.load(f)

        # clf2 — опциональный, не ломает загрузку если отсутствует
        clf2_path = os.path.join(_BASE, "clf2.pkl")
        if os.path.exists(clf2_path):
            with open(clf2_path, "rb") as f:
                _clf2 = pickle.load(f)
            logger.info("ml_score: clf2 (fast/slow breakout) загружен")

        # Load thresholds saved by train_ml.py, if available.
        _thr_path = os.path.join(_BASE, "thresholds.json")
        if os.path.exists(_thr_path):
            import json as _json
            with open(_thr_path) as _f:
                _thr = _json.load(_f)
            THRESHOLD_HIGH = float(_thr.get("THRESHOLD_HIGH", THRESHOLD_HIGH))
            THRESHOLD_LOW  = float(_thr.get("THRESHOLD_LOW",  THRESHOLD_LOW))
            logger.info("ml_score: thresholds loaded", threshold_high=round(THRESHOLD_HIGH, 4), threshold_low=round(THRESHOLD_LOW, 4))
        return True
    except Exception as e:
        logger.warning("ml_score: models not loaded", error=str(e))
        return False


async def reload_models() -> None:
    """Hot-reload models after train_ml.py finishes. Thread-safe via asyncio.Lock."""
    async with _get_lock():
        global _clf, _clf2, _reg, _le, _type_map
        _clf = None
        _clf2 = None
        _reg = None
        _le = None
        _type_map = None
        ok = _load()
        if ok:
            logger.info("ml_score: models reloaded successfully")
        else:
            logger.warning("ml_score: reload failed, models unavailable until next retry")


def ml_score(lvl: dict) -> dict:
    """
    Принимает lvl-словарь (тот же что в calculate_strength).
    Использует 5 признаков (touches и age_capped убраны).
    approach_style читается из lvl["approach_style"] (fallback: "unknown").

    Возвращает dict с ключами:
        p_bounce          — вероятность отбоя (0..1)
        expected_depth    — ожидаемый прокол под уровень в % (консервативно, is_breakout=0)
        ml_delta          — поправка к strength: +1 / 0 / -1
        p_fast_breakout   — вероятность быстрого пробоя (0..1), None если clf2 не загружен
    При ошибке загрузки возвращает нейтральный результат.
    """
    if not _load():
        return {"p_bounce": 0.5, "expected_depth": 1.5, "ml_delta": 0, "p_fast_breakout": None}

    try:
        ltype     = lvl.get("type", "body_level")
        strength  = float(lvl.get("strength", 3) or 3)
        vol       = float(lvl.get("vol_ratio", 1.0) or 1.0)
        atr_ratio = float(lvl.get("atr_ratio", 2.0) or 2.0)
        style     = lvl.get("approach_style", "unknown") or "unknown"

        ltype_enc = _type_map.get(ltype, 1)
        style_enc = STYLE_MAP.get(style, 3)

        # Вектор признаков: 5 штук — strength, ltype, vol, atr, style.
        # age_capped УДАЛЁН синхронно с train_ml.py (health-отчёт: >75% записей
        # имеют age=0, поле заполняется только при завершении монитора → мёртвый
        # признак, шум, мешал калибровке). Модель обучена на 5 фичах — вектор
        # скоринга должен точно совпадать, иначе 'X has 6 features, expecting 5'.
        x = np.array([[
            strength,
            ltype_enc,
            min(vol, 20.0),
            min(atr_ratio, 20.0),
            style_enc,
        ]])

        # ── Классификатор bounce/breakout ──────────────────────────────
        try:
            proba      = _clf.predict_proba(x)[0]
            bounce_idx = list(_le.classes_).index("bounce")
            p_bounce   = float(proba[bounce_idx])
        except Exception as e:
            logger.warning("clf predict failed (model mismatch?)", error=str(e))
            p_bounce = 0.5

        # ── Регрессор fill_depth (is_breakout=0 — консервативная оценка) ──
        try:
            x_reg = np.append(x, [[0.0]], axis=1)  # is_breakout=0
            expected_depth = float(_reg.predict(x_reg)[0])
        except Exception as e:
            logger.warning("reg predict failed (model mismatch?)", error=str(e))
            expected_depth = 1.0
        expected_depth = max(0.1, round(expected_depth, 2))

        # ── MEDIUM-1: guard against NaN leaking through silently ──────
        if math.isnan(p_bounce):
            logger.warning("clf predict_proba returned NaN, using neutral p_bounce")
            p_bounce = 0.5
        if math.isnan(expected_depth):
            logger.warning("reg predict returned NaN, using neutral expected_depth")
            expected_depth = 1.5

        # ── clf2: скорость breakout (fast/slow) ───────────────────────
        p_fast_breakout = None
        if _clf2 is not None:
            try:
                p_fast_breakout = round(float(_clf2.predict_proba(x)[0][1]), 3)
            except Exception as e:
                logger.warning("clf2 predict failed", error=str(e))

        if p_bounce >= THRESHOLD_HIGH:
            ml_delta = 1
        elif p_bounce <= THRESHOLD_LOW:
            ml_delta = -1
        else:
            ml_delta = 0

        return {
            "p_bounce":         round(p_bounce, 3),
            "expected_depth":   expected_depth,
            "ml_delta":         ml_delta,
            "p_fast_breakout":  p_fast_breakout,
        }

    except Exception as e:
        logger.warning("ml_score error", error=str(e))
        return {"p_bounce": 0.5, "expected_depth": 1.5, "ml_delta": 0, "p_fast_breakout": None}


def apply_ml_to_level(lvl: dict) -> None:
    """
    Вызвать после calculate_strength(lvl).

    Требования к lvl перед вызовом:
      - lvl["approach_style"] должен быть уже выставлен, если стиль известен.
        В _monitored(): lvl["approach_style"] = detect_approach_style(symbol)
        В _run_phase1() / _startup_monitoring(): approach_style отсутствует → "unknown"

    Hard-filter по touches СНЯТ (25.06) — G2/G4 торгуются; блок G3 (3+ подхода) живёт
    в strategy2_signal_filter. ML вызывается всегда (touches не влияет на p_bounce).

    CAP: пробитый и невосстановившийся уровень (was_broken && !sweep_reclaimed)
    — ML не корректирует вверх (только вниз или 0), чтобы не обходить штраф calculate_strength.

    Добавляет в lvl:
        p_bounce          — вероятность отбоя
        expected_depth    — ожидаемый прокол %
        ml_delta          — применённая поправка
        strength_pre_ml   — strength до ML (для логов и Claude cap)
        ml_blocked        — всегда False (hard-filter снят; ключ сохранён для совместимости)
        p_fast_breakout   — вероятность быстрого пробоя (None если clf2 не загружен)
    """
    lvl["strength_pre_ml"] = lvl.get("strength", 3)

    # ── touches: больше НЕ хард-блок ───────────────────────────────────
    # СТРАТЕГИЯ (25.06): G2 (2й подход) и G4 (флип-ретест) торгуются, поэтому хард-фильтр
    # touches>=2 → p_bounce=0 СНЯТ. Блок 3+ подходов (G3) теперь живёт в
    # strategy2_signal_filter (гейт по approach_count), а не в ML. ML отдаёт сырой
    # p_bounce по 6 признакам (touches в признаки не входит). touches считаем только
    # для логов/диагностики.
    # HIGH-2: touches_count — источник истины (trigger.get_approaching_levels);
    # "approach" — менее точный fallback из main.py/telegram.py путей.
    if "touches_count" in lvl and lvl["touches_count"] is not None:
        touches = int(lvl["touches_count"])
    else:
        touches = int(lvl.get("approach") or 1)
        logger.debug(
            "ml_score: touches_count missing, falling back to approach",
            touches=touches,
            level=lvl.get("level"),
        )

    # ml_blocked сохранён в выходе для обратной совместимости потребителей; всегда False.
    lvl["ml_blocked"] = False

    # ── ML scoring ────────────────────────────────────────────────────
    result = ml_score(lvl)

    # CAP: не повышать strength для пробитого и не восстановившегося уровня
    was_broken = lvl.get("was_broken", False)
    sweep      = lvl.get("sweep_reclaimed", False)
    if was_broken and not sweep:
        result["ml_delta"] = min(result["ml_delta"], 0)

    lvl["p_bounce"]         = result["p_bounce"]
    lvl["expected_depth"]   = result["expected_depth"]
    lvl["ml_delta"]         = result["ml_delta"]
    lvl["p_fast_breakout"]  = result["p_fast_breakout"]
    lvl["strength"]         = max(1, min(5, lvl["strength_pre_ml"] + result["ml_delta"]))

    logger.debug(
        "ml_score applied",
        level=lvl.get("level"),
        style=lvl.get("approach_style", "unknown"),
        touches=touches,
        p_bounce=round(result["p_bounce"], 2),
        expected_depth=result["expected_depth"],
        ml_delta=result["ml_delta"],
        strength_from=lvl["strength_pre_ml"],
        strength_to=lvl["strength"],
        p_fast_breakout=result["p_fast_breakout"],
    )
