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

  const modals = Object.keys(modalConfig);

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
    <div className="bg-black border-r border-yellow-100/25 text-yellow-100/70 w-64 min-h-screen p-4 flex flex-col justify-between">
      <div>
        <h1 className="text text-yellow-100/50 ml-1 mt-4">Chemical-X</h1>
        <ul>
          {modals_array.map((item) => (
            <li key={item} className="text-sm mb-1 p-1 text-yellow-100/50 rounded-lg bg-black border border-yellow-100/10 hover:bg-yellow-100/70 hover:text-black">
              <Link href={`/${item}`} className="block w-full h-full ps-1 cursor-pointer">
                {item.charAt(0) + item.slice(1)}
              </Link>
            </li>
          ))}
        </ul>
      </div>
      <button
        onClick={handleLogout}
        className="text-sm mb-1 p-3 text-yellow-100/50 rounded-lg bg-black border border-yellow-100/10 hover:bg-yellow-100/70 hover:text-black"
      >
        Logout {userName} [{userId},{userType}]
      </button>
    </div>
  );
};

export default Sidebar;

