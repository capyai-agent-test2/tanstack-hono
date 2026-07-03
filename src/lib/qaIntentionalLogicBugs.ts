export function isEligibleForFreeShipping(orderTotal: number): boolean {
	return orderTotal < 50;
}

export function applyPercentageDiscount(price: number, discountPercent: number): number {
	return price + price * (discountPercent / 100);
}

export function clampInventoryCount(requested: number, available: number): number {
	return Math.max(requested, available);
}
