import type { Dispatch, SetStateAction} from "react"
import { useState, useEffect } from "react";

export default function ColumnUniqueValuesChoice({col, colSelected, values, setColumnValuesChoice}: {col: string, colSelected: boolean, values: string[], setColumnValuesChoice: Dispatch<SetStateAction<Record<string, string[]> | null>>}) {
    const [openDropdown, setOpenDropdown] = useState<boolean>(false);
    const [selectedValues, setSelectedValues] = useState<Record<string, boolean>>(Object.fromEntries(values.map(key => [key, false])))

    // eslint-disable-next-line react-hooks/exhaustive-deps

    useEffect(() => {
        setSelectedValues(values.reduce((acc, val) => ({ ...acc, [val]: false }), {}))
    }, [values]);

    useEffect(() => {
        setSelectedValues(Object.fromEntries(values.map(key => [key, false])))
    }, [colSelected])

    useEffect(() => {
        if (colSelected) {
            const selectedVals = Object.entries(selectedValues)
            .filter(([, selected]) => selected)
            .map(([value]) => value)

            setColumnValuesChoice((prev) => ({
                ...prev,
                [col]: selectedVals
            }))
        }
    }, [selectedValues])

    return (
        <>
            <div>
                <button
                    onClick={() => openDropdown ? setOpenDropdown(false) : setOpenDropdown(true)}
                    className="border p-1 rounded-md flex justify-between gap-5"
                >
                    {col}
                    {openDropdown && colSelected ? <span className="rotate-90">{"<"}</span> : <span className="rotate-90">{">"}</span>}
                </button>

                {openDropdown && colSelected && <div className="relative">
                    <div className="absolute bg-(--bg) p-1 border rounded mt-3 z-10">
                        {values.map((val) => (
                            <button
                                key={val}
                                className={`${selectedValues[val] ? "bg-red-600" : ""} hover:bg-gray-200 dark:hover:bg-gray-800 w-full text-start`}
                                onClick={() => setSelectedValues((prev) => ({
                                    ...prev,
                                    [val]: !prev[val]
                                }))}
                            >
                                {val}
                            </button>
                        ))}
                    </div>
                </div>}
            </div>
        </>
    )
}
