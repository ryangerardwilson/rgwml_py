import React, { useEffect, useState } from 'react';
import { useRouter } from 'next/router';
import DynamicTable from '../components/DynamicTable';
import Sidebar from '../components/Sidebar';
import modalConfig from '../components/modalConfig';
import '../app/globals.css';

const ModalPage: React.FC = () => {
  const router = useRouter();
  const { modal } = router.query;

  const [data, setData] = useState<any[]>([]);
  const [columns, setColumns] = useState<string[]>([]);

  const apiHost = process.env.NEXT_PUBLIC_API_HOST;

  useEffect(() => {
    const fetchData = async () => {
      if (modal && apiHost) {
        const config = modalConfig[modal as string];
        if (config && config.read_routes && config.read_routes.length > 0) {
          const readRoute = config.read_routes[0];
	  //console.log(readRoute);
          try {
            const response = await fetch(`${apiHost}read/${modal}/${readRoute}`);
	    //console.log(response);
            if (response.ok) {
              const fetchedData = await response.json();
	      //console.log(fetchedData);
              setColumns(fetchedData.columns);
              setData(fetchedData.data);
            } else {
              setData([]);
              setColumns([]);
            }
          } catch (error) {
            setData([]);
            setColumns([]);
          }
        }
      }
    };

    fetchData();
  }, [modal, apiHost]);

  if (!modal || !apiHost) {
    return <div>Loading...</div>;
  }

  return (
    <div className="flex">
      <Sidebar />
      <div className="bg-black min-h-screen flex-1 p-8 overflow-x-auto">
        <div className="container mx-auto">
          <h1 className="text-yellow-100/50 mr-2 text-right">
            {(modal as string)} table
          </h1>
          {apiHost && (
            <DynamicTable
              apiHost={apiHost}
              modal={modal as string}
              columns={columns}
              data={data}
            />
          )}
        </div>
      </div>
    </div>
  );
};

export default ModalPage;

