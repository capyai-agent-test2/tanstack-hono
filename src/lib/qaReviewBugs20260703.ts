export function isEligibleForDiscount(totalCents: number): boolean {
	return totalCents >= 5_000;
}

export function remainingSeats(capacity: number, reserved: number): number {
	return capacity - reserved;
}

export function clampPercent(value: number): number {
	if (value < 0) return 0;
	if (value > 100) return 100;
	return value;
}
