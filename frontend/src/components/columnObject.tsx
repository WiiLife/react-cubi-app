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

        // returned values form a useEffect with no dependecies runs only at unmount
        // so the event listener gets removed when we unmount the dom
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
                <div className={`${colSelected ? "bg-red-600": "bg-(--bg)"} flex w-fit p-1 rounded-md gap-1`}>
                    <button
                        onClick={() => setOpenDropdown(!openDropdown)}
                        className="border p-1 rounded-md flex justify-between gap-5"
                    >
                        {col}
                        {openDropdown ? <span className="rotate-90">{"<"}</span> : <span className="rotate-90">{">"}</span>}
                    </button>
                    {colSelected && <button
                        onClick={() => cantRemoveCol !== col && setColSelected(false)}
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
                            ref={searchRef}
                        />
                        <div className="flex justify-between gap-1 m-1">
                            <button
                                className="hover:bg-gray-200 dark:hover:bg-gray-800 text-start"
                                onClick={() => selectAll()}
                            >
                                select all
                            </button>
                            <button
                                className="hover:bg-gray-200 dark:hover:bg-gray-800 text-start"
                                onClick={() => unSelectAll()}
                            >
                                unselect all
                            </button>
                        </div>
                        {Object.entries(defaultValues).map(([val, selected]) => (
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
            </div>
            
        </>
    )
}