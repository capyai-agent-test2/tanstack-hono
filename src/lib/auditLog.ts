export interface AuditLogEntry {
	actor: string;
	action: string;
	entityType: string;
	entityId: string;
	timestamp: string;
}

export type AuditLogRecordInput = Omit<AuditLogEntry, "timestamp"> & {
	timestamp?: string | Date;
};

const entries: AuditLogEntry[] = [];

function normalizeTimestamp(timestamp: string | Date | undefined): string {
	if (!timestamp) return new Date().toISOString();
	return timestamp instanceof Date ? timestamp.toISOString() : timestamp;
}

export function recordAuditLog(input: AuditLogRecordInput): AuditLogEntry {
	const entry: AuditLogEntry = {
		actor: input.actor,
		action: input.action,
		entityType: input.entityType,
		entityId: input.entityId,
		timestamp: normalizeTimestamp(input.timestamp),
	};

	entries.push(entry);
	return entry;
}

export interface ListAuditLogOptions {
	actor?: string;
	limit?: number;
}

export function listAuditLog(options: ListAuditLogOptions = {}): AuditLogEntry[] {
	const cappedLimit = Math.min(Math.max(options.limit ?? 100, 0), 100);
	const filteredEntries = options.actor
		? entries.filter((entry) => entry.actor === options.actor)
		: entries;

	return [...filteredEntries]
		.sort((left, right) => Date.parse(right.timestamp) - Date.parse(left.timestamp))
		.slice(0, cappedLimit)
		.map((entry) => ({ ...entry }));
}

export function clearAuditLog(): void {
	entries.length = 0;
}
