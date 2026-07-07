import { describe, expect, it } from "vite-plus/test";

import {
	applyDiscountCents,
	calculateSubtotalCents,
	calculateTaxCents,
	type ReviewSeedLineItem,
} from "../lib/reviewSeed";

const items: ReviewSeedLineItem[] = [
	{ priceCents: 1299, quantity: 2, taxable: true },
	{ priceCents: 500, quantity: 3, taxable: false },
];

describe("review seed helpers", () => {
	it("includes quantity when calculating subtotal", () => {
		expect(calculateSubtotalCents(items)).toBe(4098);
	});

	it("converts basis points to percent correctly", () => {
		expect(calculateTaxCents(items, 875)).toBe(227);
	});

	it("does not allow discounts to create negative totals", () => {
		expect(applyDiscountCents(500, 750)).toBe(0);
	});
});
