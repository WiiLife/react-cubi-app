import { useState } from "react"


export default function TableChoice({tableNames, setTableChoice}: {tableNames: {"tables": string[]}, setTableChoice: (table: string) => void}) {
    const [openDropdown, setOpenDropdown] = useState<boolean>(false);
    const [table, setTable] = useState<string>("choose a table");
    
    return (
        <>
            <div className="w-1/2">
                <button
                    onClick={() => openDropdown ? setOpenDropdown(false) : setOpenDropdown(true)}
                    className="border p-1 rounded-md flex justify-between w-9/12"
                >
                    {table}
                    {openDropdown ? <span className="rotate-90">{"<"}</span> : <span className="rotate-90">{">"}</span>}
                </button>

                <div className="relative">
                    {openDropdown && <div className="absolute bg-(--bg) border rounded mt-3">
                        {tableNames.tables.map((table) => (
                        <div key={table}>
                            <button className="w-full text-left px-1 py-1 hover:bg-gray-200 dark:hover:bg-gray-800" 
                                onClick={() => {setTableChoice(table); setTable(table); setOpenDropdown(false);}}
                            >
                                {table}
                            </button>
                        </div>
                        ))}
                    </div>}
                </div>
            </div>
        </>
    )
}
