const normalizeSurfaceInput = (value) => {
  if (typeof value === 'number' && Number.isFinite(value)) return `${value}`;
  if (typeof value !== 'string') return '';
  return value.replace(/m²|mq/ig, '').replace(/[^\d,.-]/g, '').trim();
};

const parseLocalizedSurfaceNumber = (value) => {
  const cleaned = normalizeSurfaceInput(value);
  if (!cleaned) return null;

  const lastComma = cleaned.lastIndexOf(',');
  const lastDot = cleaned.lastIndexOf('.');
  const decimalIndex = Math.max(lastComma, lastDot);

  if (decimalIndex === -1) {
    const parsed = Number.parseFloat(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
  }

  const decimalDigits = cleaned.slice(decimalIndex + 1).replace(/[^\d]/g, '');
  const hasExplicitDecimal = decimalDigits.length > 0 && decimalDigits.length <= 2;
  const decimalSeparator = decimalIndex === lastComma ? ',' : '.';
  const thousandsSeparator = decimalSeparator === ',' ? '.' : ',';
  const normalized = hasExplicitDecimal
    ? `${cleaned.slice(0, decimalIndex).replace(new RegExp(`\\${thousandsSeparator}`, 'g'), '')}.${decimalDigits}`
    : cleaned.replace(/[.,]/g, '');
  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) ? parsed : null;
};

export const parseSurfaceNumber = (value) => {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const cleaned = normalizeSurfaceInput(value);
  if (!cleaned) return null;

  const separators = cleaned.match(/[.,]/g) || [];
  if (separators.length === 1) {
    const separator = separators[0];
    const separatorIndex = cleaned.indexOf(separator);
    const integerPart = cleaned.slice(0, separatorIndex).replace(/[^\d-]/g, '');
    const trailingDigits = cleaned.slice(separatorIndex + 1).replace(/[^\d]/g, '');
    if (integerPart && trailingDigits.length === 3 && integerPart.replace('-', '').length <= 2) {
      const normalized = `${integerPart}${trailingDigits.slice(0, 1)}.${trailingDigits.slice(1)}`;
      const parsed = Number.parseFloat(normalized);
      return Number.isFinite(parsed) ? parsed : null;
    }
  }

  return parseLocalizedSurfaceNumber(cleaned);
};
