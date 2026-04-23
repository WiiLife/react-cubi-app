
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


export function getTables(): Promise<string[]> {
    return api(`${BACKEND}/api/tables`);
}

export function getColumnValues(table: string): Promise<Record<string, string[]>>  {
    return api(`${BACKEND}/api/tables/${table}/columns-values`)
}

export function getTable({table, columns, columnVariables} : 
    {table: string, columns: Record<string, string[]>, columnVariables: string[]}
): Promise<Record<string, unknown>[]> {
    return api(`${BACKEND}/api/tables/${table}/pivot`, {
        method: "POST",
        body: JSON.stringify({columns, column_variables: columnVariables})
    })
} 
