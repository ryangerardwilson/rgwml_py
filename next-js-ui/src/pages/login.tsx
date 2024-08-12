import React, { useState } from 'react';
import { useRouter } from 'next/navigation';
import '../app/globals.css';


const isAuthenticated = () => {
  const cookies = document.cookie.split(';').reduce((acc: { [key: string]: string }, cookie) => {
    const [key, value] = cookie.trim().split('=');
    acc[key] = value;
    return acc;
  }, {} as { [key: string]: string });

  return cookies.user_id !== undefined;
};


const LoginPage: React.FC = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const router = useRouter();
  const apiHost = process.env.NEXT_PUBLIC_API_HOST;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (isAuthenticated()) {
      router.push('/');
      return;
    }

    try {
      const response = await fetch(`${apiHost}authenticate`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username, password }),
      });

      const result = await response.json();
      console.log(result); // Debugging: Log the result
      if (result.status === 'success') {
        // Set cookie for authentication
        const user = result.user;
        document.cookie = `user_id=${user.id}; path=/`;
	document.cookie = `username=${user.username}; path=/`;
	document.cookie = `type=${user.type}; path=/`;
        document.cookie = `auth=true; path=/`;
        console.log("Authentication successful, redirecting..."); // Debugging: Log before redirect
        router.push('/');
      } else {
        setError(result.message);
      }
    } catch (error) {
      console.error("Error during authentication:", error);
      setError("An error occurred during authentication. Please try again.");
    }
  };

  return (
    <div className="min-h-screen bg-black text-yellow-100/50 flex items-center justify-center">
      <div className="border border-yellow-100/30 p-7 rounded-lg shadow-lg w-96">
        <h2 className="mb-4 text-center font-bold animate-pulse">Chemical-X</h2>
        <form onSubmit={handleSubmit}>
          <div className="mb-4">
            <label className="block text-sm text-yellow-100/50 ml-1">Username</label>
            <input
              type="text"
              name="username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="bg-black p-2 rounded-lg w-full border border-yellow-100/30 text-sm"
              required
            />
          </div>
          <div className="mb-2">
            <label className="block text-sm text-yellow-100/50 ml-1">Password</label>
            <input
              type="password"
              name="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="bg-black p-2 rounded-lg w-full border border-yellow-100/30 text-sm"
              required
            />
          </div>
          {error && <p className="text-red-500 mb-2">{error}</p>}
          <div className="flex justify-end mt-8">
            <button
              type="submit"
              className="bg-black hover:bg-yellow-100/50 text-yellow-100/50 hover:text-black py-2 px-4 rounded-lg text-sm border border-yellow-100/30 hover:border-black"
            >
              Login
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default LoginPage;