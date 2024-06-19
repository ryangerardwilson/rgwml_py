// src/app/page.tsx
import React from 'react';
import { redirect } from 'next/navigation';

const HomePage: React.FC = () => {

  const modals = process.env.MODALS.split(',');

  // Perform server-side redirection
  redirect(`/${modals[0]}`);

  return null; // No need to return any JSX as the redirection happens immediately
};

export default HomePage;

