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

  const getUserIDFromCookies = (): string | undefined => {
    const cookies = document.cookie.split(';').reduce((acc: { [key: string]: string }, cookie) => {
      const [key, value] = cookie.trim().split('=');
      acc[key] = value;
      return acc;
    }, {} as { [key: string]: string });

    return cookies.user_id;
  };

useEffect(() => {
  const fetchData = async () => {
    if (modal && apiHost) {
      const config = modalConfig[modal as string];
      if (config && config.read_routes) {
        const userId = getUserIDFromCookies();

        //console.log("UserID from cookies:", userId);

        for (const readRouteKey in config.read_routes) {
          if (config.read_routes.hasOwnProperty(readRouteKey)) {
            const readRoute = config.read_routes[readRouteKey];
            //console.log("ReadRoute Key:", readRouteKey);
            //console.log("ReadRoute Config:", readRoute);
            //console.log("UserID before constructing URL:", userId);

            // Construct URL based on belongs_to_user_id flag
            const apiUrl = readRoute.belongs_to_user_id && userId
              ? `${apiHost}read/${modal}/${readRouteKey}/${userId}`
              : `${apiHost}read/${modal}/${readRouteKey}`;

            //console.log('Constructed API URL:', apiUrl);

            try {
              const response = await fetch(apiUrl);
              if (response.ok) {
                const fetchedData = await response.json();
                setColumns(fetchedData.columns);
                setData(fetchedData.data);
                break; // exit the loop once we have a successful response
              } else {
                setData([]);
                setColumns([]);
              }
            } catch (error) {
              //console.error("Data fetch error:", error);
              setData([]);
              setColumns([]);
            }
          }
        }
      }
    }
  };

  // Ensure this runs only on client-side
  if (typeof window !== 'undefined') {
    fetchData();
  }
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

