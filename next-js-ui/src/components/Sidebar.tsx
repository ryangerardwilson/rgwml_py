import React from 'react';
import Link from 'next/link';
import { useRouter } from 'next/router';
import modalConfig from './modalConfig';

const parseCookies = () => {
  return document.cookie.split(';').reduce((acc: { [key: string]: string }, cookie) => {
    const [key, value] = cookie.trim().split('=');
    acc[key] = value;
    return acc;
  }, {} as { [key: string]: string });
};


const Sidebar: React.FC = () => {
  const cookies = parseCookies();
  const userType = cookies.type;
  const userName = cookies.username;
  const userId = cookies.user_id;
  

  // Get the modals from the configuration
  const modals = Object.keys(modalConfig);

  // Create the modals_array and conditionally remove "users" if the user type is not "admin" or "sudo"
  const modals_array = modals.filter(modal => {
    if (modal === 'users' && userType !== 'admin' && userType !== 'sudo') {
      return false;
    }
    return true;
  });

  const router = useRouter();
  const { modal } = router.query;

  const handleLogout = () => {
    document.cookie = 'user_id=; Max-Age=0; path=/';
    document.cookie = 'auth=; Max-Age=0; path=/';
    document.cookie = 'username=; Max-Age=0; path=/';
    document.cookie = 'type=; Max-Age=0; path=/'
    router.push('/login');
  };

  return (
    <div className="bg-gray-800 text-gray-100 w-64 min-h-screen p-4 flex flex-col justify-between">
      <div>
        <h2 className="text-2xl font-semibold mb-6">Menu</h2>
        <ul>
          {modals_array.map((item) => (
            <li key={item} className={`mb-4 p-2 rounded ${modal === item ? 'bg-gray-700' : 'hover:bg-gray-700 transition duration-300'}`}>
              <Link href={`/${item}`}>
                <span className="text-white cursor-pointer">{item.charAt(0).toUpperCase() + item.slice(1)}</span>
              </Link>
            </li>
          ))}
        </ul>
      </div>
      <div className="mt-auto mb-4">
        <p className="text-sm">Logged in: {userName} [id: {userId}, type: {userType}]</p>
      </div>
      <button
        onClick={handleLogout}
        className="bg-red-500 hover:bg-red-700 text-white py-2 px-4 rounded focus:outline-none focus:ring-2 focus:ring-red-500"
      >
        Logout
      </button>
    </div>
  );
};

export default Sidebar;


