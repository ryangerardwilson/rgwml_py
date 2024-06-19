# Generated file: frontend_assets.py

DIR__APP__FILE__PAGE__TSX = '''// src/app/page.tsx
import React from 'react';
import {{ redirect }} from 'next/navigation';

const HomePage: React.FC = () => {{

  const modals = process.env.MODALS.split(',');

  // Perform server-side redirection
  redirect(`/${{modals[0]}}`);

  return null; // No need to return any JSX as the redirection happens immediately
}};

export default HomePage;'''

DIR__APP__FILE__LAYOUT__TSX = '''import type {{ Metadata }} from "next";
import {{ Inter }} from "next/font/google";
import "./globals.css";

const inter = Inter({{ subsets: ["latin"] }});

export const metadata: Metadata = {{
  title: "Create Next App",
  description: "Generated by create next app",
}};

export default function RootLayout({{
  children,
}}: Readonly<{{
  children: React.ReactNode;
}}>) {{
  return (
    <html lang="en">
      <body className={{inter.className}}>{{children}}</body>
    </html>
  );
}}'''

DIR__APP__FILE__GLOBALS__CSS = '''@tailwind base;
@tailwind components;
@tailwind utilities;'''

DIR__COMPONENTS__FILE__CREATE_MODAL__TSX = '''import React, {{ useState, useEffect }} from 'react';

interface CreateModalProps {{
  modalName: string;
  apiHost: string;
  columns: string[];
  onClose: () => void;
}}

const CreateModal: React.FC<CreateModalProps> = ({{ modalName, apiHost, columns, onClose }}) => {{
  const [formData, setFormData] = useState<{{ [key: string]: any }}>({{}});
  const [userId, setUserId] = useState<string | null>(null);

  useEffect(() => {{
    // Extract user_id from cookies
    const cookies = document.cookie.split(';').reduce((acc, cookie) => {{
      const [key, value] = cookie.trim().split('=');
      acc[key] = value;
      return acc;
    }}, {{}} as {{ [key: string]: string }});

    setUserId(cookies.user_id || null);
  }}, []);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {{
    setFormData({{
      ...formData,
      [e.target.name]: e.target.value
    }});
  }};

  const handleSubmit = async () => {{
    try {{
      const dataToSubmit = {{ ...formData, user_id: userId }};
      console.log('35',dataToSubmit);
      const response = await fetch(`${{apiHost}}create/${{modalName}}`, {{
        method: 'POST',
        headers: {{
          'Content-Type': 'application/json'
        }},
        body: JSON.stringify(dataToSubmit)
      }});
      if (response.ok) {{
        alert('Record created successfully');
        onClose();
	 window.location.reload();
      }} else {{
        alert('Failed to create record');
      }}
    }} catch (error) {{
      console.error('Error creating record:', error);
    }}
  }};

  const filteredColumns = columns.filter(column => !['id', 'created_at', 'updated_at', 'user_id'].includes(column));

  return (
    <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50">
      <div className="bg-gray-800 p-8 rounded-lg shadow-lg text-white w-96">
        <h2 className="text-2xl mb-4">Create New {{modalName.charAt(0).toUpperCase() + modalName.slice(1)}}</h2>
        {{filteredColumns.map((column) => (
          <input
            key={{column}}
            type="text"
            name={{column}}
            placeholder={{column}}
            onChange={{handleChange}}
            className="bg-gray-700 p-2 rounded mb-4 w-full"
          />
        ))}}
        <div className="flex justify-end">
          <button onClick={{onClose}} className="bg-red-500 hover:bg-red-700 text-white py-2 px-4 rounded mr-2">Cancel</button>
          <button onClick={{handleSubmit}} className="bg-green-500 hover:bg-green-700 text-white py-2 px-4 rounded">Create</button>
        </div>
      </div>
    </div>
  );
}};

export default CreateModal;'''

DIR__COMPONENTS__FILE__SIDEBAR__TSX = '''import React from 'react';
import Link from 'next/link';
import {{ useRouter }} from 'next/router';

const Sidebar: React.FC = () => {{
  const modals = ['customers', 'partners'];
  const router = useRouter();
  const {{ modal }} = router.query;

  const handleLogout = () => {{
    document.cookie = 'user_id=; Max-Age=0; path=/';
    document.cookie = 'auth=; Max-Age=0; path=/';
    router.push('/login');
  }};

  return (
    <div className="bg-gray-800 text-gray-100 w-64 min-h-screen p-4 flex flex-col justify-between">
      <div>
        <h2 className="text-2xl font-semibold mb-6">Menu</h2>
        <ul>
          {{modals.map((item) => (
            <li key={{item}} className={{`mb-4 p-2 rounded ${{modal === item ? 'bg-gray-700' : 'hover:bg-gray-700 transition duration-300'}}`}}>
              <Link href={{`/${{item}}`}}>
                <span className="text-white cursor-pointer">{{item.charAt(0).toUpperCase() + item.slice(1)}}</span>
              </Link>
            </li>
          ))}}
        </ul>
      </div>
      <button
        onClick={{handleLogout}}
        className="bg-red-500 hover:bg-red-700 text-white py-2 px-4 rounded focus:outline-none focus:ring-2 focus:ring-red-500"
      >
        Logout
      </button>
    </div>
  );
}};

export default Sidebar;'''

DIR__COMPONENTS__FILE__DYNAMIC_TABLE__TSX = '''import React, {{ useState, useEffect }} from 'react';
import CreateModal from './CreateModal';
import EditModal from './EditModal';

interface DynamicTableProps {{
  apiHost: string;
  modal: string;
  columns: string[];
  data: any[];
}}

const DynamicTable: React.FC<DynamicTableProps> = ({{ apiHost, modal, columns, data: initialData }}) => {{
  const [data, setData] = useState<any[]>(initialData);
  const [isCreateModalOpen, setCreateModalOpen] = useState(false);
  const [isEditModalOpen, setEditModalOpen] = useState(false);
  const [editRowData, setEditRowData] = useState<{{ [key: string]: any }} | null>(null);
  const [filterQuery, setFilterQuery] = useState(''); // State for the filter query

  useEffect(() => {{
    setData(initialData);
  }}, [initialData]);

  useEffect(() => {{
    if (!initialData.length) {{
      const fetchData = async () => {{
        try {{
          const response = await fetch(`${{apiHost}}read/${{modal}}`);
          const result = await response.json();
          setData(result.data || []);
        }} catch (error) {{
          console.error('Error fetching data:', error);
          setData([]);
        }}
      }};
      fetchData();
    }}
  }}, [apiHost, modal, initialData]);

  const handleDelete = async (id: number, userId: number) => {{
    try {{
      const response = await fetch(`${{apiHost}}delete/${{modal}}/${{id}}`, {{
        method: 'DELETE',
        headers: {{
          'Content-Type': 'application/json',
        }},
        body: JSON.stringify({{ user_id: userId }}),
      }});
      const result = await response.json();
      if (result.status === 'success') {{
        setData(data.filter((row: any) => row[0] !== id)); // Assuming the first column is the ID
      }}
    }} catch (error) {{
      console.error('Error deleting data:', error);
    }}
  }};

  const handleEdit = (row: {{ [key: string]: any }}) => {{
    setEditRowData(row);
    setEditModalOpen(true);
  }};

  const handleCreate = () => {{
    setCreateModalOpen(true);
  }};

  const closeCreateModal = () => setCreateModalOpen(false);

  const closeEditModal = (updatedData: any[] | null) => {{
    if (updatedData) {{
      // Update the rowData with the new data
      setData(prevData => prevData.map(row => row[0] === updatedData[0] ? updatedData : row));
    }}
    setEditModalOpen(false);
  }};

  const handleFilterChange = (event: React.ChangeEvent<HTMLInputElement>) => {{
    setFilterQuery(event.target.value);
  }};

  const evaluateFilter = (row: any, query: string): boolean => {{
    const regex = /(\w+)\s*(CONTAINS|STARTS_WITH|==|>|<|>=|<=)\s*'([^']*)'/i;
    const match = query.match(regex);

    if (!match) return true;

    const [, column, operator, value] = match;
    const columnIndex = columns.indexOf(column);

    if (columnIndex === -1) return false;

    const cellValue = row[columnIndex];

    switch (operator) {{
      case 'CONTAINS':
        return cellValue.toString().toLowerCase().includes(value.toLowerCase());
      case 'STARTS_WITH':
        return cellValue.toString().toLowerCase().startsWith(value.toLowerCase());
      case '==':
        return cellValue.toString() === value;
      case '>':
        return new Date(cellValue) > new Date(value) || parseFloat(cellValue) > parseFloat(value);
      case '<':
        return new Date(cellValue) < new Date(value) || parseFloat(cellValue) < parseFloat(value);
      case '>=':
        return new Date(cellValue) >= new Date(value) || parseFloat(cellValue) >= parseFloat(value);
      case '<=':
        return new Date(cellValue) <= new Date(value) || parseFloat(cellValue) <= parseFloat(value);
      default:
        return true;
    }}
  }};

  const filteredData = data.filter(row => evaluateFilter(row, filterQuery));

  const formatDateTime = (dateTime: string) => {{
    const date = new Date(dateTime);
    const year = date.getFullYear();
    const month = ('0' + (date.getMonth() + 1)).slice(-2);
    const day = ('0' + date.getDate()).slice(-2);
    const hours = ('0' + date.getHours()).slice(-2);
    const minutes = ('0' + date.getMinutes()).slice(-2);
    const seconds = ('0' + date.getSeconds()).slice(-2);
    return `${{year}}-${{month}}-${{day}} ${{hours}}:${{minutes}}:${{seconds}}`;
  }};

  return (
    <div className="bg-gray-900 text-white p-4">
      <div className="flex justify-between mb-4">
        <input
          type="text"
          value={{filterQuery}}
          onChange={{handleFilterChange}}
          placeholder="Filter [==, >, >=, <, <=, CONTAINS, STARTS_WITH] ..."
          className="bg-gray-800 text-white px-4 py-2 rounded w-1/3"
        />
        <button
          onClick={{handleCreate}}
          className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
        >
          Create
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-700">
          <thead>
            <tr>
              {{columns.map((col, colIndex) => (
                <th
                  key={{`col-${{colIndex}}`}}
                  className="px-3 py-3 text-left text-xs font-medium text-gray-300 tracking-wider"
                >
                  {{col}}
                </th>
              ))}}
              <th className="px-3 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">Actions</th>
            </tr>
          </thead>
          <tbody>
            {{filteredData.map((row, rowIndex) => (
              <tr key={{`row-${{rowIndex}}`}} className="bg-gray-800">
                {{row.map((cell: any, cellIndex: number) => (
                  <td
                    key={{`cell-${{rowIndex}}-${{cellIndex}}`}}
                    className="px-3 py-4 whitespace-nowrap text-sm text-gray-300"
                  >
                    {{typeof cell === 'string' && !isNaN(Date.parse(cell)) ? formatDateTime(cell) : cell}}
                  </td>
                ))}}
                <td className="px-3 py-4 whitespace-nowrap text-sm text-gray-300">
                  <button
                    onClick={{() => handleEdit(row)}}
                    className="bg-yellow-500 hover:bg-yellow-700 text-white font-bold py-1 px-2 rounded mr-2"
                  >
                    Edit
                  </button>
                  <button
                    onClick={{() => handleDelete(row[0], row.user_id)}} // Pass the user_id here
                    className="bg-red-500 hover:bg-red-700 text-white font-bold py-1 px-2 rounded"
                  >
                    Delete
                  </button>
                </td>
              </tr>
            ))}}
          </tbody>
        </table>
      </div>
      {{isCreateModalOpen && <CreateModal modalName={{modal}} apiHost={{apiHost}} columns={{columns}} onClose={{closeCreateModal}} />}}
      {{isEditModalOpen && editRowData && <EditModal modalName={{modal}} apiHost={{apiHost}} columns={{columns}} rowData={{editRowData}} onClose={{closeEditModal}} />}}
    </div>
  );
}};

export default DynamicTable;'''

DIR__COMPONENTS__FILE__EDIT_MODAL__TSX = '''import React, {{ useState, useEffect }} from 'react';

interface EditModalProps {{
  modalName: string;
  apiHost: string;
  columns: string[];
  rowData: any[];
  onClose: (updatedData: any[]) => void; // Modify onClose to accept updated data
}}

const EditModal: React.FC<EditModalProps> = ({{ modalName, apiHost, columns, rowData, onClose }}) => {{
  const [formData, setFormData] = useState<{{ [key: string]: any }}>({{}});

  useEffect(() => {{
    // Convert rowData to an object with column names as keys
    const initialFormData = columns.reduce((acc, column, index) => {{
      acc[column] = rowData[index];
      return acc;
    }}, {{}} as {{ [key: string]: any }});
    setFormData(initialFormData);
  }}, [columns, rowData]);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {{
    setFormData({{
      ...formData,
      [e.target.name]: e.target.value
    }});
  }};

  const handleSubmit = async () => {{
    try {{
      const response = await fetch(`${{apiHost}}update/${{modalName}}/${{formData.id}}`, {{
        method: 'PUT',
        headers: {{
          'Content-Type': 'application/json'
        }},
        body: JSON.stringify(formData)
      }});
      if (response.ok) {{
        alert('Record updated successfully');
        onClose(Object.values(formData)); // Pass updated data to onClose
      }} else {{
        alert('Failed to update record');
      }}
    }} catch (error) {{
      console.error('Error updating record:', error);
    }}
  }};

  return (
    <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50">
      <div className="bg-gray-800 p-8 rounded-lg shadow-lg text-white w-96">
        <h2 className="text-2xl mb-4">
          Edit {{modalName.charAt(0).toUpperCase() + modalName.slice(1)}} ({{rowData[0]}})
        </h2>
        {{columns.map((column) => (
          column !== 'created_at' && column !== 'updated_at' && column !== 'id' && column !== 'user_id' && (
            <div key={{column}} className="mb-4">
              <label className="block text-sm font-medium text-gray-300">{{column}}</label>
              <input
                type="text"
                name={{column}}
                value={{formData[column] || ''}}
                placeholder={{column}}
                onChange={{handleChange}}
                className="bg-gray-700 p-2 rounded w-full"
              />
            </div>
          )
        ))}}
        <div className="flex justify-end">
          <button onClick={{() => onClose(null)}} className="bg-red-500 hover:bg-red-700 text-white py-2 px-4 rounded mr-2">Cancel</button>
          <button onClick={{handleSubmit}} className="bg-green-500 hover:bg-green-700 text-white py-2 px-4 rounded">Update</button>
        </div>
      </div>
    </div>
  );
}};

export default EditModal;'''

DIR__PAGES__FILE__LOGIN__TSX = '''import React, {{ useState }} from 'react';
import {{ useRouter }} from 'next/navigation';
import '../app/globals.css';

const isAuthenticated = () => {{
  const cookies = document.cookie.split(';').reduce((acc, cookie) => {{
    const [key, value] = cookie.trim().split('=');
    acc[key] = value;
    return acc;
  }}, {{}});

  return cookies.user_id !== undefined;
}};

const LoginPage: React.FC = () => {{
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const router = useRouter();
  const apiHost = process.env.NEXT_PUBLIC_API_HOST;

  const handleSubmit = async (e: React.FormEvent) => {{
    e.preventDefault();

    if (isAuthenticated()) {{
      router.push('/');
      return;
    }}

    try {{
      const response = await fetch(`${{apiHost}}/authenticate`, {{
        method: 'POST',
        headers: {{
          'Content-Type': 'application/json',
        }},
        body: JSON.stringify({{ username, password }}),
      }});

      const result = await response.json();
      console.log(result); // Debugging: Log the result
      if (result.status === 'success') {{
        // Set cookie for authentication
        const user = result.user;
        document.cookie = `user_id=${{user.id}}; path=/`;
        document.cookie = `auth=true; path=/`;
        console.log("Authentication successful, redirecting..."); // Debugging: Log before redirect
        router.push('/');
      }} else {{
        setError(result.message);
      }}
    }} catch (error) {{
      console.error("Error during authentication:", error);
      setError("An error occurred during authentication. Please try again.");
    }}
  }};

  return (
    <div className="min-h-screen bg-gray-900 text-white flex items-center justify-center">
      <div className="bg-gray-800 p-8 rounded-lg shadow-lg w-96">
        <h2 className="text-2xl mb-4 text-center font-semibold">Login</h2>
        <form onSubmit={{handleSubmit}}>
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-300">Username</label>
            <input
              type="text"
              name="username"
              value={{username}}
              onChange={{(e) => setUsername(e.target.value)}}
              className="bg-gray-700 p-2 rounded w-full focus:outline-none focus:ring-2 focus:ring-green-500"
              required
            />
          </div>
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-300">Password</label>
            <input
              type="password"
              name="password"
              value={{password}}
              onChange={{(e) => setPassword(e.target.value)}}
              className="bg-gray-700 p-2 rounded w-full focus:outline-none focus:ring-2 focus:ring-green-500"
              required
            />
          </div>
          {{error && <p className="text-red-500 mb-4">{{error}}</p>}}
          <div className="flex justify-end">
            <button
              type="submit"
              className="bg-green-500 hover:bg-green-700 text-white py-2 px-4 rounded focus:outline-none focus:ring-2 focus:ring-green-500"
            >
              Login
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}};

export default LoginPage;'''

DIR__PAGES__FILE__PAGE__TSX = ''''''

DIR__PAGES__FILE____MODAL____TSX = '''import React, {{ useEffect, useState }} from 'react';
import {{ useRouter }} from 'next/router';
import DynamicTable from '../components/DynamicTable';
import Sidebar from '../components/Sidebar';
import '../app/globals.css';

const ModalPage: React.FC = () => {{
  const router = useRouter();
  const {{ modal }} = router.query;

  const [data, setData] = useState<any[]>([]);
  const [columns, setColumns] = useState<string[]>([]);

  const apiHost = process.env.NEXT_PUBLIC_API_HOST;

  useEffect(() => {{
    console.log("ModalPage useEffect called");
    console.log("modal:", modal);
    console.log("apiHost:", apiHost);

    if (modal && apiHost) {{
      const fetchData = async () => {{
        try {{
          console.log(`Fetching data from ${{apiHost}}read/${{modal}}`);
          const response = await fetch(`${{apiHost}}read/${{modal}}`);
          console.log("Response received:", response);

          if (response.ok) {{
            const fetchedData = await response.json();
            console.log("Fetched data:", fetchedData);
            setColumns(fetchedData.columns);
            setData(fetchedData.data);
          }} else {{
            console.error("Error response:", response.statusText);
            setData([]);
            setColumns([]);
          }}
        }} catch (error) {{
          console.error(`Error fetching data for ${{modal}}:`, error);
          setData([]);
          setColumns([]);
        }}
      }};

      fetchData();
    }}
  }}, [modal, apiHost]);

  if (!modal) {{
    return <div>Loading...</div>;
  }}

  return (
    <div className="flex">
      <Sidebar />
      <div className="bg-black min-h-screen flex-1 p-8">
        <div className="container mx-auto">
          <h1 className="text-4xl text-white font-bold mb-4">{{(modal as string)?.charAt(0).toUpperCase() + (modal as string)?.slice(1)}} Management</h1>
          <DynamicTable apiHost={{apiHost}} modal={{modal as string}} columns={{columns}} data={{data}} />
        </div>
      </div>
    </div>
  );
}};

export default ModalPage;'''

ROOT__FILE__MIDDLEWARE__TSX = '''import {{ NextResponse }} from 'next/server';
import type {{ NextRequest }} from 'next/server';

export function middleware(request: NextRequest) {{
  const user_id = request.cookies.get('user_id');

  if (!user_id && request.nextUrl.pathname !== '/login') {{
    return NextResponse.redirect(new URL('/login', request.url));
  }}

  if (user_id && request.nextUrl.pathname === '/login') {{
    return NextResponse.redirect(new URL('/', request.url));
  }}

  return NextResponse.next();
}}

const modals = process.env.MODALS.split(',')

export const config = {{


  matcher: ['/', '/customers', '/partners', '/login'],
}};'''

