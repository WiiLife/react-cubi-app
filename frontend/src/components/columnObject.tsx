import { useEffect, useState } from "react";

export default function ColumnObject({col, values, defaultSelected, setColValues, togglePivotColumn}: 
    {
        col: string, 
        values: Record<string, boolean>, 
        defaultSelected: boolean,
        setColValues: (col: string, value: string) => void,
        togglePivotColumn: (col: string, method: "toggle" | "pivot" | "row") => void
    }) {
    const [colSelected, setColSelected] = useState<boolean>(defaultSelected);
    const [openDropdown, setOpenDropdown] = useState<boolean>(false);

    // eslint-disable-next-line react-hooks/exhaustive-deps

    useEffect(() => {
        if (!colSelected) {
            togglePivotColumn(col, "pivot");
            Object.entries(values).filter(([, selected]) => selected).map(([val,]) => setColValues(col, val));
        };
    }, [colSelected])

    useEffect(() => {
        const hasSelectedValues = Object.entries(values).some(([, selected]) => selected);
        setColSelected(hasSelectedValues);
    }, [values])

    return (
        <>
            <div>
                COL: {col}
            </div>
            <div>   
                SELECTED: {colSelected ? "true": "false"}
            </div>
            <div>
                TRUE VALUES: {Object.entries(values).filter(([, isSelected]) => isSelected).map(([val]) => val).join(", ")}
            </div>
            <div className={`${colSelected ? "bg-red-600": ""} flex w-fit p-1 rounded-md gap-1`}>
                <button
                    onClick={() => openDropdown ? setOpenDropdown(false) : setOpenDropdown(true)}
                    className="border p-1 rounded-md flex justify-between gap-5"
                >
                    {col}
                    {openDropdown ? <span className="rotate-90">{"<"}</span> : <span className="rotate-90">{">"}</span>}
                </button>
                {colSelected && <button
                    onClick={() => setColSelected(false)}
                    className="hover:bg-red-900 p-1 rounded-md"
                >
                    remove
                </button>}
            </div>
            {openDropdown && <div className="relative">
                <div className="absolute p-1 bg-(--bg) border rounded mt-3 z-10">
                    {Object.entries(values).map(([val, selected]) => (
                        <button
                            key={val}
                            className={`${selected ? "bg-red-600" : ""} hover:bg-gray-200 dark:hover:bg-gray-800 w-full text-start`}
                            onClick={() => setColValues(col, val)}
                        >
                            {val}
                        </button>
                    ))}
                </div>
            </div>}
        </>
    )
}