import FilterDisplay from "./components/filterDisplay"
import { useEffect, useState } from "react"
import type { Props } from "./interfaces/props"
import { getTables } from "./api/api"
import { getColumns } from "./api/api"


function App() {
  const [tableChoice, setTableChoice] = useState<string>();
  const [columns, setColumnsChoice] = useState<string[] | null>(null);
  const [props, setProps] = useState<Props>({
    tableNames: {"tables": []},
    setTableChoice,
    columns,
    setColumnsChoice,
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
    async function fetchColumns(table: string) {
      const columns = await getColumns(table);
      setProps((prev) => ({
        ...prev,
        columns
      }))
    }

    if (tableChoice) {
      fetchColumns(tableChoice) 
    }
    
  }, [tableChoice])

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
          COLUMN CHOICES: {columns}
        </div>

      </div>
    </>
  )
}

export default App
