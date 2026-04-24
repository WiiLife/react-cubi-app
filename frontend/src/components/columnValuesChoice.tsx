import { useState, useEffect} from "react";
import type { Props } from "../interfaces/props";
import type { Dispatch, SetStateAction } from "react";


export default function ColumnUniqueValuesChoice({props, col, colSelected, setColSelected, colValues}: 
    {props: Props, col: string, colSelected: boolean, setColSelected: Dispatch<SetStateAction<Record<string, boolean>>>, colValues: Record<string, string[]>}) {
    const [openDropdown, setOpenDropdown] = useState<boolean>(false);
    const values = colValues[col] ?? [];
    const [selectedValues, setSelectedValues] = useState<Record<string, boolean>>(Object.fromEntries(values.map((key, i) => [key, i === 0])))

    useEffect(() => {
        // eslint-disable-next-line react-hooks/exhaustive-deps
        setSelectedValues(Object.fromEntries(values.map((key, i) => [key, i === 0])))
    }, [values])

    useEffect(() => {
        if (Object.values(selectedValues).some((value) => value)) {
            setColSelected((prev) => ({
                ...prev,
                [col]: true
            }))
        } else {
            setColSelected((prev) => ({
                ...prev,
                [col]: false
            }))
        }
    }, [selectedValues])

    useEffect(() => {
        if (!colSelected) {
            setSelectedValues(Object.fromEntries(values.map(key => [key, false])))
        } 
    }, [colSelected])

    useEffect(() => {
        const selectedValuesList = Object.entries(selectedValues)
        .filter(([, selected]) => selected)
        .map(([value]) => value)
        props.setColumnValuesChoice((prev) => ({
            ...prev,
            [col]: selectedValuesList
        }))
    }, [selectedValues])


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

                {openDropdown && <div className="relative">
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
