import { useState } from "react"

export default function ColumnUniqueValuesChoice({col}: {col: string}) {
    const [openDropdown, setOpenDropdown] = useState<boolean>(false);

    return (
        <>
            <div>
                <button
                    onClick={() => openDropdown ? setOpenDropdown(false) : setOpenDropdown(true)}
                    className="border p-1 rounded-md flex justify-between gap-5"
                >
                    {col}
                    {openDropdown ? <span className="rotate-90">{"<"}</span> : <span className="rotate-90">{">"}</span>}
                </button>

                <div className="relative">
                    {openDropdown && 
                    <div className="absolute bg-(--bg) p-1 border rounded mt-3">
                        <p>1 value</p>
                        <p>2 value</p>
                        <p>3 value</p>
                        <p>4 value</p>
                        <p>5 value</p>
                    </div>} 
                </div>
            </div>
        </>
    )
}
