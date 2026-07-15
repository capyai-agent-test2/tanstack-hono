export type AuditLogEntry = {
	actor: string;
	action: string;
	entityType: string;
	entityId: string;
	timestamp: string;
};

export type RecordAuditLogEntryInput = Omit<AuditLogEntry, "timestamp"> & {
	timestamp?: Date | string;
};

const auditLogEntries: AuditLogEntry[] = [];

const normalizeLimit = (limit?: number) => {
	if (!Number.isFinite(limit) || limit === undefined) {
		return 100;
	}

	return Math.min(Math.max(Math.trunc(limit), 0), 100);
};

export const recordAuditLogEntry = (entry: RecordAuditLogEntryInput) => {
	const auditLogEntry = {
		...entry,
		timestamp:
			entry.timestamp instanceof Date
				? entry.timestamp.toISOString()
				: entry.timestamp || new Date().toISOString(),
	};

	auditLogEntries.push(auditLogEntry);

	return auditLogEntry;
};

export const listAuditLogEntries = ({
	actor,
	limit,
}: {
	actor?: string;
	limit?: number;
} = {}) => {
	return [...auditLogEntries]
		.filter((entry) => (actor ? entry.actor === actor : true))
		.sort((a, b) => Date.parse(b.timestamp) - Date.parse(a.timestamp))
		.slice(0, normalizeLimit(limit));
};

export const clearAuditLogEntries = () => {
	auditLogEntries.length = 0;
};
