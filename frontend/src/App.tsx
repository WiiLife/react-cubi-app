import FilterDisplay from "./components/filterDisplay"
import { useEffect, useState } from "react"
import type { Props } from "./interfaces/props"
import { getTables } from "./api/api"
import { getColumnValues } from "./api/api"
import TableCard from "./components/tableCard"


function App() {
  const [tableChoice, setTableChoice] = useState<string | null>(null);
  const [columnValuesChoice, setColumnValuesChoice] = useState<Record<string, string[]> | null>(null);
  const [props, setProps] = useState<Props>({
    tableNames: null,
    tableChoice,
    setTableChoice,
    columns: null,
    columnValues: null,
    columnValuesChoice,
    setColumnValuesChoice,
  });

  useEffect(() => {
    async function fetchTables() {
      const tableNames = await getTables();
      setProps((prev) => ({
        ...prev,
        tableNames
      }));
    }

    fetchTables();
  }, [])

  useEffect(() => {
    async function fetchColumnValues(table: string) {
      const columnValues = await getColumnValues(table);
      setProps((prev) => ({
        ...prev,
        columns: Object.keys(columnValues),
        columnValues
      }))
    }

    if (tableChoice) {
      fetchColumnValues(tableChoice) 
    }
  }, [tableChoice])

  useEffect(() => {

  }, [props.columnValuesChoice, tableChoice])

  return (
    <>
      <div className="h-screen flex flex-col max-w-3/4 mx-auto">
          <div className="bg-red-600 pb-7 p-3 flex">
            <div>
              <div></div>
              <h1>REACT CUBI APP</h1>
              <p className="max-w-1/2 p-2">this is a react application frontend with a python backend engine for the a cubi app first developed internally as an R shiny app for USTAT</p>
              <p className="flex font-bold gap-2">Backend running on:<a href={import.meta.env.VITE_API_URL} className="hover:text-neutral-300">{import.meta.env.VITE_API_URL}</a></p>
            </div>
            <div className="p-3">
              references
            </div>
          </div>
          
          
          <div className="m-5">
            <FilterDisplay props={props}/>
          </div>

        <div>
          TABLE CHOICE: {tableChoice}
        </div>
        <div>
          COLUMN CHOICES: {columnValuesChoice ? Object.keys(columnValuesChoice).join(', ') : null}
        </div>
        <div>
          VALUES CHOICES:
          {columnValuesChoice && Object.entries(columnValuesChoice).map(([col, values]) => (
            <div key={col} className="ml-4">
              {col}: {values.join(', ')}
            </div>
          ))}
        </div>

        <div className="m-5">
          {tableChoice && columnValuesChoice && <TableCard table={tableChoice} columnValues={columnValuesChoice}/>}
        </div>

      </div>
    </>
  )
}

export default App
