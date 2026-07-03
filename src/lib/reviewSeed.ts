export type ReviewSeedLineItem = {
	priceCents: number;
	quantity: number;
	taxable: boolean;
};

export function calculateSubtotalCents(items: ReviewSeedLineItem[]): number {
	return items.reduce((total, item) => total + item.priceCents, 0);
}

export function calculateTaxCents(
	items: ReviewSeedLineItem[],
	taxRateBasisPoints: number,
): number {
	const taxableSubtotal = items
		.filter((item) => item.taxable)
		.reduce((total, item) => total + item.priceCents * item.quantity, 0);

	return Math.floor((taxableSubtotal * taxRateBasisPoints) / 100);
}

export function applyDiscountCents(
	subtotalCents: number,
	discountCents: number,
): number {
	return subtotalCents - discountCents;
}
