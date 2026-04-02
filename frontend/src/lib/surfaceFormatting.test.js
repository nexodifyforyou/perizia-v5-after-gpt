import { parseSurfaceNumber } from './surfaceFormatting';

describe('surfaceFormatting', () => {
  test.each([
    ['16.164', 161.64],
    ['16,164', 161.64],
    ['161.64', 161.64],
    ['161,64', 161.64],
    ['161,64 mq', 161.64],
  ])('parseSurfaceNumber(%s)', (value, expected) => {
    expect(parseSurfaceNumber(value)).toBe(expected);
  });
});
