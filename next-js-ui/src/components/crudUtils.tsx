// src/components/crudUtils.tsx
import modalConfig from './modalConfig';

export const handleCreate = (setCreateModalOpen: (open: boolean) => void) => {
  setCreateModalOpen(true);
};

export const closeCreateModal = (setCreateModalOpen: (open: boolean) => void) => {
  setCreateModalOpen(false);
};

const getUserIDFromCookies = (): string | undefined => {
  const cookies = document.cookie.split(';').reduce((acc: { [key: string]: string }, cookie) => {
    const [key, value] = cookie.trim().split('=');
    acc[key] = value;
    return acc;
  }, {} as { [key: string]: string });

  return cookies.user_id;
};

export const fetchData = async (
  apiHost: string, 
  modal: string, 
  routeKey: string, 
  setData: React.Dispatch<React.SetStateAction<any[]>>, 
  setColumns: React.Dispatch<React.SetStateAction<any[]>>
) => {

  const config = modalConfig[modal];
  const userId = getUserIDFromCookies();

  try {
    const readRoute = config.read_routes[routeKey];
    const apiUrl = readRoute.belongs_to_user_id && userId
      ? `${apiHost}read/${modal}/${routeKey}/${userId}`
      : `${apiHost}read/${modal}/${routeKey}`;

    const response = await fetch(apiUrl);
    const result = await response.json();
    setColumns(result.columns || []);
    setData(result.data || []);
  } catch (error) {
    setColumns([]);
    setData([]);
  }
};


export const handleDelete = async (apiHost: string, modal: string, id: number, userId: number) => {
  try {
    const response = await fetch(`${apiHost}delete/${modal}/${id}`, {
      method: 'DELETE',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ user_id: userId }),
    });
    const result = await response.json();
    /*
    if (result.status === 'success') {
      setData(data.filter((row: any) => row[0] !== id));
    }
    */
  } catch (error) {
    console.error('Error deleting data:', error);
  }
};



export const handleEdit = (
  row: { [key: string]: any },
  setEditRowData: React.Dispatch<React.SetStateAction<any[]>>, // Expect an array
  setEditModalOpen: React.Dispatch<React.SetStateAction<boolean>>
) => {
  setEditRowData([row]); // Wrap the row in an array
  setEditModalOpen(true);
};

export const closeEditModal = (
  updatedData: any[] | null,
  columns: string[],
  setData: React.Dispatch<React.SetStateAction<any[]>>,
  setEditModalOpen: React.Dispatch<React.SetStateAction<boolean>>
) => {
  if (updatedData && updatedData.length > 0) {
    //console.log('56', updatedData);
    const updatedRow = updatedData[0]; // Assuming updatedData contains an array with a single object

    const updatedId = updatedRow.id; // Assuming the row object has an 'id' property
    //console.log('60', updatedId);

    setData(prevData => {
      const newData = prevData.map(row => {
        // Assuming `row` is an array and the first element is the id
        if (row[0] === updatedId) {
          return columns.map(column => updatedRow[column] !== undefined ? updatedRow[column] : row[columns.indexOf(column)]);
        }
        return row;
      });
      //console.log('72', newData);
      return newData;
    });
  }
  setEditModalOpen(false);
};

