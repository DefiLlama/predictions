export function parseEpochToDate(epoch: number): Date {
  const milliseconds = epoch > 1_000_000_000_000 ? epoch : epoch * 1000;
  return new Date(milliseconds);
}
