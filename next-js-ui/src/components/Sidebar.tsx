// src/components/Sidebar.tsx

import React, { useEffect, useCallback, useState } from 'react';
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
  const [cookies, setCookies] = useState<Cookies>({});
  const [userType, setUserType] = useState<string>('');
  const [userName, setUserName] = useState<string>('');
  const [userId, setUserId] = useState<string>('');

  const modals = Object.keys(modalConfig);

  const filteredModals = modals.filter(modal => {
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

  const handleBulkOperationsClick = () => {
    router.push('/bulk_operations');
  };

  useEffect(() => {
    if (typeof window !== 'undefined') {
      const parsedCookies = parseCookies();
      setCookies(parsedCookies);
      setUserType(parsedCookies.type);
      setUserName(parsedCookies.username);
      setUserId(parsedCookies.user_id);

      if (!parsedCookies.user_id) {
        handleLogout();
      }
    }
  }, [handleLogout]);

  const apkUrl = process.env.NEXT_PUBLIC_APK_URL;

  return (
    <div className="bg-black border-r border-yellow-100/25 text-yellow-100/70 w-64 min-h-screen p-4 flex flex-col">
      <div className="flex-grow">
        <h1 className="text text-yellow-100/50 ml-1 mt-4">Chemical-X</h1>
         <hr className="border-yellow-100/10 my-4"/>
        <ul>
          {filteredModals.map((item) => (
            <li key={item} className="text-sm mb-1 p-1 text-yellow-100/50 rounded-lg bg-black border border-yellow-100/10 hover:bg-yellow-100/70 hover:text-black">
              <Link href={`/${item}`} legacyBehavior>
                <a className="block w-full h-full ps-1 cursor-pointer">
                  {item}
                </a>
              </Link>
            </li>
          ))}
        </ul>
        <hr className="border-yellow-100/10 my-4"/>
        {userType === 'admin' || userType === 'sudo' ? (
          <button
            onClick={handleBulkOperationsClick}
            className="text-sm mb-1 p-1 text-yellow-100/50 rounded-lg bg-black border border-yellow-100/10 hover:bg-yellow-100/70 hover:text-black w-full text-left"
          >
            bulk_operations
          </button>
        ) : null}
        <a
          className="block text-sm mb-1 p-1 text-yellow-100/50 rounded-lg bg-black border border-yellow-100/10 hover:bg-yellow-100/70 hover:text-black text-left"
          href={apkUrl}
          download
        >
          download_android_app
        </a>
        <button
          onClick={handleLogout}
          className="text-sm mb-1 p-1 text-yellow-100/50 rounded-lg bg-black border border-yellow-100/10 hover:bg-yellow-100/70 hover:text-black w-full text-left"
        >
          logout_{userName}_[{userId},{userType}]
        </button>
      </div>
    </div>
  );
};

export default Sidebar;