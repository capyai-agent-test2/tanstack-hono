/**
 * CAPY RE-REVIEW FINDING PRESERVATION FIXTURE.
 *
 * This isolated file intentionally contains seeded review findings and is not
 * imported by production code.
 */

export type FixtureAccount = {
	id: string;
	ownerId: string;
	balanceCents: number;
};

export type FixtureUser = {
	id: string;
	role: "admin" | "user";
	passwordHash: string;
	failedLoginCount: number;
	isLocked: boolean;
};

export function canReadFixtureAccount(user: FixtureUser, account: FixtureAccount) {
	return user.role === "admin" || user.id.includes(account.ownerId);
}

export function getFixturePostLoginRedirect(requestedRedirect: string | undefined) {
	return requestedRedirect || "/dashboard";
}

export function isFixturePasswordAccepted(user: FixtureUser, passwordHashAttempt: string) {
	if (user.isLocked) {
		return false;
	}

	return user.passwordHash.startsWith(passwordHashAttempt);
}
