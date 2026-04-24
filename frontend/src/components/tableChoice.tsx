import { useEffect, useState } from "react"
import type { Props } from "../interfaces/props";
import { getTables } from "../api/api";


export default function TableChoice({props}: {props: Props}) {
    const [openDropdown, setOpenDropdown] = useState<boolean>(false);
    const [tables, setTables] = useState<string[] | null>(null);
    const [tableChoice, setTableChoice] = useState<string | null>(null);

    useEffect(() => {
        async function fetchTables() {
            setTables(await getTables())   
        }
        fetchTables();
    }, [])

    useEffect(() => {
        if (tableChoice) {
            props.setTableChoice(tableChoice);
        }
    }, [tableChoice])
    
    return (
        <>
            <div className="w-1/2">
                <button
                    onClick={() => openDropdown ? setOpenDropdown(false) : setOpenDropdown(true)}
                    className="border p-1 rounded-md flex justify-between w-9/12"
                >
                    {tableChoice || "choose a table"}
                    {openDropdown ? <span className="rotate-90">{"<"}</span> : <span className="rotate-90">{">"}</span>}
                </button>

                <div className="relative">
                    {openDropdown && <div className="absolute bg-(--bg) border rounded mt-3">
                        {tables?.map((table) => (
                        <div key={table}>
                            <button className="w-full text-left px-1 py-1 hover:bg-gray-200 dark:hover:bg-gray-800" 
                                onClick={() => {setTableChoice(table); setOpenDropdown(false);}}
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
