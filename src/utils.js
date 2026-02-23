function toE164Spain(raw) {
  const s = String(raw || "").replace(/\s+/g, "").replace(/-/g, "");
  if (!s) return null;
  if (s.startsWith("+")) return s;
  if (/^\d{9}$/.test(s)) return `+34${s}`;
  return null;
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function addHours(date, hours) {
  return new Date(date.getTime() + hours * 3600 * 1000);
}

function applySendWindow(date, startHour, endHour) {
  const d = new Date(date);
  const h = d.getHours();
  if (h < startHour) {
    d.setHours(startHour, randInt(0, 59), randInt(0, 59), 0);
    return d;
  }
  if (h >= endHour) {
    d.setDate(d.getDate() + 1);
    d.setHours(startHour, randInt(0, 59), randInt(0, 59), 0);
    return d;
  }
  return d;
}

function computeNextSendFrom(lastOutboundIso, baseHours, cfg) {
  const last = new Date(lastOutboundIso);
  let next = addHours(last, baseHours + randInt(cfg.JITTER_MIN_HOURS, cfg.JITTER_MAX_HOURS));
  next = applySendWindow(next, cfg.SEND_WINDOW_START, cfg.SEND_WINDOW_END);
  return next.toISOString();
}

/**
 * Pacing: cuántos MSG1 “deberías llevar” enviados a esta hora para repartir uniforme.
 */
function expectedNewSendsByNow(now, cfg) {
  const start = new Date(now);
  start.setHours(cfg.SEND_WINDOW_START, 0, 0, 0);

  const end = new Date(now);
  end.setHours(cfg.SEND_WINDOW_END, 0, 0, 0);

  if (now < start) return 0;
  if (now >= end) return cfg.DAILY_NEW_LIMIT;

  const elapsed = now.getTime() - start.getTime();
  const total = end.getTime() - start.getTime();
  const ratio = elapsed / total;
  return Math.floor(cfg.DAILY_NEW_LIMIT * ratio);
}

module.exports = { toE164Spain, computeNextSendFrom, expectedNewSendsByNow };

function addHoursIso(iso, hours) {
  const d = new Date(iso);
  return new Date(d.getTime() + hours * 3600 * 1000).toISOString();
}

module.exports = { toE164Spain, computeNextSendFrom, expectedNewSendsByNow, addHoursIso };