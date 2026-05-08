import { useEffect, useRef, useState } from "react";

export default function ColumnObject({col, values, cantRemoveCol, defaultSelected, setColValues, togglePivotColumn}: 
    {
        col: string, 
        values: Record<string, boolean>, 
        cantRemoveCol: string | null,
        defaultSelected: boolean,
        setColValues: (col: string, value: string) => void,
        togglePivotColumn: (col: string, method: "toggle" | "pivot" | "row") => void
    }) {
    const [colSelected, setColSelected] = useState<boolean>(defaultSelected);
    const [defaultValues, setDefaultValues] = useState<Record<string, boolean>>(values);
    const [openDropdown, setOpenDropdown] = useState<boolean>(false);
    const searchRef = useRef<HTMLInputElement>(null);
    const dropDownRef = useRef<HTMLDivElement>(null);

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
        setDefaultValues(values);
        if (searchRef.current) {
            searchRef.current.value = "";
        }
    }, [values])

    useEffect(() => {
        const handleClickOutside = (e: MouseEvent) => {
            if (dropDownRef.current && !dropDownRef.current.contains(e.target as Node)) {
                setOpenDropdown(false)
            }
        }
        document.addEventListener('click', handleClickOutside);
        
        return () => {
            document.removeEventListener('click', handleClickOutside)
        };
    }, []);

    const selectAll = () => {
        Object.entries(values).filter(([, selected]) => !selected).map(([val,]) => setColValues(col, val))
    }

    const unSelectAll = () => {
        Object.entries(values).filter(([, selected]) => selected).map(([val,]) => setColValues(col, val))
    }

    const search = (keyword: string) => {
        setDefaultValues(Object.fromEntries(Object.entries(values).filter(([val,]) => val.toLocaleLowerCase().includes(keyword.toLocaleLowerCase()))))
    }

    return (
        <>
            <div className="relative" ref={dropDownRef}>
                <div className={`${colSelected ? "bg-red-600": "bg-gray-700"} flex items-center w-fit p-1 rounded-md gap-1`}>
                    <div>
                        <svg xmlns="http://www.w3.org/2000/svg" fill="currentColor" className="bi bi-grip-vertical w-6 h-6" viewBox="0 0 16 16">
                            <path d="M7 2a1 1 0 1 1-2 0 1 1 0 0 1 2 0m3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0M7 5a1 1 0 1 1-2 0 1 1 0 0 1 2 0m3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0M7 8a1 1 0 1 1-2 0 1 1 0 0 1 2 0m3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0m-3 3a1 1 0 1 1-2 0 1 1 0 0 1 2 0m3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0m-3 3a1 1 0 1 1-2 0 1 1 0 0 1 2 0m3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0"/>
                        </svg>
                    </div>
                    <button
                        onClick={() => setOpenDropdown(!openDropdown)}
                        onMouseDown={(e) => e.stopPropagation()}
                        className="border p-1 rounded-md flex justify-between gap-5"
                    >
                        {col}
                        {openDropdown ? <span className="rotate-90">{"<"}</span> : <span className="rotate-90">{">"}</span>}
                    </button>
                    {colSelected && <button
                        onClick={() => cantRemoveCol !== col && setColSelected(false)}
                        onMouseDown={(e) => e.stopPropagation()}
                        className="hover:bg-red-900 p-1 rounded-md"
                    >
                        remove
                    </button>}
                </div>
                {openDropdown && <div>
                    <div className="absolute flex flex-col p-1 bg-(--bg) border rounded mt-2 z-10">
                        <input
                            className="hover:bg-gray-200 dark:hover:bg-gray-800 text-start"
                            placeholder="search"
                            onChange={(e) => search(e.target.value)}
                            onMouseDown={(e) => e.stopPropagation()}
                            ref={searchRef}
                        />
                        <div className="flex justify-between gap-1 m-1">
                            <button
                                className="hover:bg-gray-200 dark:hover:bg-gray-800 text-start"
                                onClick={() => selectAll()}
                                onMouseDown={(e) => e.stopPropagation()}
                            >
                                select all
                            </button>
                            <button
                                className="hover:bg-gray-200 dark:hover:bg-gray-800 text-start"
                                onClick={() => unSelectAll()}
                                onMouseDown={(e) => e.stopPropagation()}
                            >
                                unselect all
                            </button>
                        </div>
                        {Object.entries(defaultValues).map(([val, selected]) => (
                            <button
                                key={val}
                                className={`${selected ? "bg-red-600" : ""} hover:bg-gray-200 dark:hover:bg-gray-800 w-full text-start`}
                                onClick={() => setColValues(col, val)}
                                onMouseDown={(e) => e.stopPropagation()}
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