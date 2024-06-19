import React from 'react';
import Link from 'next/link';
import { useRouter } from 'next/router';

const Sidebar: React.FC = () => {
  const modals = ['customers', 'partners'];
  const router = useRouter();
  const { modal } = router.query;

  const handleLogout = () => {
    document.cookie = 'user_id=; Max-Age=0; path=/';
    document.cookie = 'auth=; Max-Age=0; path=/';
    router.push('/login');
  };

  return (
    <div className="bg-gray-800 text-gray-100 w-64 min-h-screen p-4 flex flex-col justify-between">
      <div>
        <h2 className="text-2xl font-semibold mb-6">Menu</h2>
        <ul>
          {modals.map((item) => (
            <li key={item} className={`mb-4 p-2 rounded ${modal === item ? 'bg-gray-700' : 'hover:bg-gray-700 transition duration-300'}`}>
              <Link href={`/${item}`}>
                <span className="text-white cursor-pointer">{item.charAt(0).toUpperCase() + item.slice(1)}</span>
              </Link>
            </li>
          ))}
        </ul>
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

