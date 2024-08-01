import React, { useEffect, useCallback } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/router';
import modalConfig from './modalConfig';

interface Cookies {
  [key: string]: string;
}

const parseCookies = (): Cookies => {
  return document.cookie.split(';').reduce((acc: Cookies, cookie) => {
    const [key, value] = cookie.trim().split('=');
    acc[key] = value;
    return acc;
  }, {});
};

const Sidebar: React.FC = () => {
  const cookies = parseCookies();
  const userType = cookies.type;
  const userName = cookies.username;
  const userId = cookies.user_id;

  const modals = Object.keys(modalConfig);

  const modalsArray = modals.filter(modal => {
    if (modal === 'users' && userType !== 'admin' && userType !== 'sudo') {
      return false;
    }
    return true;
  });

  const router = useRouter();

  const handleLogout = useCallback(() => {
    document.cookie = 'user_id=; Max-Age=0; path=/';
    document.cookie = 'auth=; Max-Age=0; path=/';
    document.cookie = 'username=; Max-Age=0; path=/';
    document.cookie = 'type=; Max-Age=0; path=/';
    router.push('/login');
  }, [router]);

  useEffect(() => {
    if (!userId) {
      handleLogout();
    }
  }, [handleLogout, userId]);

  const apkUrl = process.env.NEXT_PUBLIC_APK_URL;

  return (
    <div className="bg-black border-r border-yellow-100/25 text-yellow-100/70 w-64 min-h-screen p-4 flex flex-col justify-between">
      <div>
        <h1 className="text text-yellow-100/50 ml-1 mt-4">Chemical-X</h1>
        <ul>
          {modalsArray.map((item) => (
            <li key={item} className="text-sm mb-1 p-1 text-yellow-100/50 rounded-lg bg-black border border-yellow-100/10 hover:bg-yellow-100/70 hover:text-black">
              <Link href={`/${item}`} legacyBehavior>
                <a className="block w-full h-full ps-1 cursor-pointer">
                  {item}
                </a>
              </Link>
            </li>
          ))}
        </ul>
      </div>
      <div>
        <a
          className="block text-sm mb-2 p-3 text-yellow-100/50 rounded-lg bg-black border border-yellow-100/10 hover:bg-yellow-100/70 hover:text-black text-center"
          href={apkUrl}
          download
        >
          Download Android App
        </a>
        <button
          onClick={handleLogout}
          className="text-sm mb-1 p-3 text-yellow-100/50 rounded-lg bg-black border border-yellow-100/10 hover:bg-yellow-100/70 hover:text-black w-full"
        >
          Logout {userName} [{userId},{userType}]
        </button>
      </div>
    </div>
  );
};

export default Sidebar;

