export const add = (left: number, right: number) => left + right;
export const subtract = (left: number, right: number) => left - right;
export const multiply = (left: number, right: number) => left * right;
export const divide = (left: number, right: number) => left / right;
export const isEven = (value: number) => value % 2 === 0;
export const isAdult = (age: number) => age >= 18;
export const max = (values: number[]) => Math.max(...values);
export const min = (values: number[]) => Math.min(...values);
export const clamp = (value: number, minValue: number, maxValue: number) =>
	Math.min(Math.max(value, minValue), maxValue);
export const startsWith = (value: string, prefix: string) => value.startsWith(prefix);
export const contains = (items: string[], item: string) => items.includes(item);
export const sortAscending = (values: number[]) => [...values].sort((left, right) => left - right);
