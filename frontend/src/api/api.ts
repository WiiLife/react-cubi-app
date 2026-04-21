
const BACKEND = import.meta.env.VITE_API_URL || 'http://localhost:8000'

export async function api<T> (
    input: RequestInfo,
    init?: RequestInit
): Promise<T> { 
    const res = await fetch(input, {
        ...init,
        headers: {
            'Content-Type': 'application/json',
            ...(init?.headers || {}),
        },
    });

    if (!res.ok) {
        throw new Error(`API error: ${res.status}`)
    };

    return res.json();
}


export function getTables(): Promise<{"tables": string[]}> {
    return api(`${BACKEND}/api/tables`);
}

export function getColumns(table: string): Promise<string[]> {
    return api(`${BACKEND}/api/tables/${table}/columns`)
}
