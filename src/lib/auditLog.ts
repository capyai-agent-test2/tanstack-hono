export interface AuditLogEntry {
	actor: string;
	action: string;
	entityType: string;
	entityId: string;
	timestamp: string;
}

export interface AuditLogEntryInput {
	actor: string;
	action: string;
	entityType: string;
	entityId: string;
	timestamp?: Date | string;
}

export interface ListAuditLogEntriesOptions {
	actor?: string;
	limit?: number;
}

interface StoredAuditLogEntry extends AuditLogEntry {
	sequence: number;
}

const maxAuditLogLimit = 100;
const entries: StoredAuditLogEntry[] = [];
let nextSequence = 0;

function normalizeLimit(limit = maxAuditLogLimit): number {
	if (!Number.isFinite(limit)) return maxAuditLogLimit;
	return Math.min(Math.max(Math.trunc(limit), 0), maxAuditLogLimit);
}

function toAuditLogEntry(entry: StoredAuditLogEntry): AuditLogEntry {
	return {
		actor: entry.actor,
		action: entry.action,
		entityType: entry.entityType,
		entityId: entry.entityId,
		timestamp: entry.timestamp,
	};
}

export function recordAuditLogEntry(input: AuditLogEntryInput): AuditLogEntry {
	const storedEntry: StoredAuditLogEntry = {
		actor: input.actor,
		action: input.action,
		entityType: input.entityType,
		entityId: input.entityId,
		timestamp:
			input.timestamp instanceof Date
				? input.timestamp.toISOString()
				: (input.timestamp ?? new Date().toISOString()),
		sequence: nextSequence,
	};

	nextSequence += 1;
	entries.push(storedEntry);

	return toAuditLogEntry(storedEntry);
}

export function listAuditLogEntries(options: ListAuditLogEntriesOptions = {}): AuditLogEntry[] {
	const limit = normalizeLimit(options.limit);

	return [...entries]
		.filter((entry) => !options.actor || entry.actor === options.actor)
		.sort((left, right) => {
			const timestampDelta =
				new Date(right.timestamp).getTime() - new Date(left.timestamp).getTime();
			return timestampDelta || right.sequence - left.sequence;
		})
		.slice(0, limit)
		.map(toAuditLogEntry);
}

export function clearAuditLogEntries(): void {
	entries.length = 0;
	nextSequence = 0;
}
