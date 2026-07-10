interface ReactRefreshGlobals {
	$RefreshReg$?: (type: unknown, id: string) => void;
	$RefreshSig$?: () => <T>(type: T) => T;
}

const globals = globalThis as typeof globalThis & ReactRefreshGlobals;

globals.$RefreshReg$ = () => {};
globals.$RefreshSig$ = () => (type) => type;
