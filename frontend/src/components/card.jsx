// src/components/card.jsx
import React from 'react';

export default function Card({ children, className }) {
  return (
    <div className={className}>
      {children}
    </div>
  );
}
