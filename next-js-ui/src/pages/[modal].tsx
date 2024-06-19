import React, { useEffect, useState } from 'react';
import { useRouter } from 'next/router';
import DynamicTable from '../components/DynamicTable';
import Sidebar from '../components/Sidebar';
import '../app/globals.css';

const ModalPage: React.FC = () => {
  const router = useRouter();
  const { modal } = router.query;

  const [data, setData] = useState<any[]>([]);
  const [columns, setColumns] = useState<string[]>([]);

  const apiHost = process.env.NEXT_PUBLIC_API_HOST;

  useEffect(() => {
    console.log("ModalPage useEffect called");
    console.log("modal:", modal);
    console.log("apiHost:", apiHost);

    if (modal && apiHost) {
      const fetchData = async () => {
        try {
          console.log(`Fetching data from ${apiHost}read/${modal}`);
          const response = await fetch(`${apiHost}read/${modal}`);
          console.log("Response received:", response);

          if (response.ok) {
            const fetchedData = await response.json();
            console.log("Fetched data:", fetchedData);
            setColumns(fetchedData.columns);
            setData(fetchedData.data);
          } else {
            console.error("Error response:", response.statusText);
            setData([]);
            setColumns([]);
          }
        } catch (error) {
          console.error(`Error fetching data for ${modal}:`, error);
          setData([]);
          setColumns([]);
        }
      };

      fetchData();
    }
  }, [modal, apiHost]);

  if (!modal) {
    return <div>Loading...</div>;
  }

  return (
    <div className="flex">
      <Sidebar />
      <div className="bg-black min-h-screen flex-1 p-8">
        <div className="container mx-auto">
          <h1 className="text-4xl text-white font-bold mb-4">{(modal as string)?.charAt(0).toUpperCase() + (modal as string)?.slice(1)} Management</h1>
          <DynamicTable apiHost={apiHost} modal={modal as string} columns={columns} data={data} />
        </div>
      </div>
    </div>
  );
};

export default ModalPage;

