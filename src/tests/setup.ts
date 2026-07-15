const refreshGlobal = globalThis as typeof globalThis & {
	$RefreshReg$: () => void;
	$RefreshSig$: () => <T>(type: T) => T;
};

refreshGlobal.$RefreshReg$ = () => {};
refreshGlobal.$RefreshSig$ = () => (type) => type;
