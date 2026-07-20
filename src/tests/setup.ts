const globalRefresh = globalThis as typeof globalThis & {
	$RefreshReg$?: () => void;
	$RefreshSig$?: () => () => void;
};

globalRefresh.$RefreshReg$ = () => {};
globalRefresh.$RefreshSig$ = () => () => {};
