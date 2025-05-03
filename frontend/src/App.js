// frontend/src/App.js
import React, { useState } from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import { Moon, Sun, TrendingUp, Beaker } from 'lucide-react';


// Page imports:
import Home from './pages/Home';
import CveMentions from './pages/CveMentions';
import ScienceEnvironment from './pages/ScienceEnvironment';
import BusinessFinanceTrade from './pages/BusinessFinanceTrade';
import AiMachineLearning from './pages/AiMachineLearning';
import CybersecurityDataPrivacy from './pages/CybersecurityDataPrivacy';
import PoliticsGovernment from './pages/PoliticsGovernment';
import ConsumerTechGadgets from './pages/ConsumerTechGadgets';
import AutomotiveSpaceTransportation from './pages/AutomotiveSpaceTransportation';
import EnterpriseCloudComputing from './pages/EnterpriseCloudComputing';
import Other from './pages/Other';

// Example time ranges:
const TIME_RANGES = [
  { label: 'Last 24 hours', value: 24 },
  { label: 'Last 8 hours',  value: 8 },
  { label: 'Last hour',     value: 1 },
  { label: 'Last 7 days',   value: 168 },
  { label: 'Last 1 month',  value: 720 },
  { label: 'All Time',      value: 0 },
];

function App() {
  const [darkMode, setDarkMode] = useState(true);
  const [selectedTimeRange, setSelectedTimeRange] = useState(TIME_RANGES[0].value);

  return (
    <Router>
      <div
        className={`min-h-screen transition-colors duration-300 ease-in-out ${
          darkMode ? 'bg-gray-900 text-gray-100' : 'bg-gray-50 text-gray-800'
        }`}
      >
        {/* HEADER / NAV */}
        <header
          className={`border-b p-4 transition-colors duration-300 ease-in-out ${
            darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'
          }`}
        >
          <div className="container mx-auto flex items-center justify-between">
            {/* Left side: Title & Nav links */}
            <div className="flex items-center gap-6">
              <h1 className="text-xl font-bold flex items-center gap-2">
                🛡️ Security News Dashboard
              </h1>
              <nav className="flex flex-wrap items-center gap-4">
                <Link to="/" className="hover:underline">
                  Home
                </Link>
                <Link to="/cve" className="hover:underline">
                  CVE Mentions
                </Link>
                <Link to="/science-environment" className="hover:underline">
                  Science &amp; Environment
                </Link>
                <Link to="/business-finance-trade" className="hover:underline">
                  Business, Finance &amp; Trade
                </Link>
                <Link to="/ai-ml" className="hover:underline">
                  AI &amp; ML
                </Link>
                <Link to="/cybersecurity-privacy" className="hover:underline">
                  Cybersecurity &amp; Data Privacy
                </Link>
                <Link to="/politics-government" className="hover:underline">
                  Politics &amp; Government
                </Link>
                <Link to="/consumer-tech-gadgets" className="hover:underline">
                  Consumer Tech &amp; Gadgets
                </Link>
                <Link to="/automotive-space-transportation" className="hover:underline">
                  Automotive, Space &amp; Transport
                </Link>
                <Link to="/enterprise-cloud-computing" className="hover:underline">
                  Enterprise &amp; Cloud
                </Link>
                <Link to="/other" className="hover:underline">
                  Other
                </Link>
              </nav>
            </div>

            {/* Right side: Dark Mode & Time Range */}
            <div className="flex items-center gap-4">
              {/* Dark Mode Toggle */}
              <button
                onClick={() => setDarkMode(!darkMode)}
                className={`p-2 rounded-full transition-all duration-300 ease-in-out
                  hover:bg-opacity-80 hover:scale-110 ${
                    darkMode ? 'text-gray-200 hover:bg-gray-700' : 'hover:bg-gray-200'
                  }`}
              >
                {darkMode ? <Sun className="w-5 h-5" /> : <Moon className="w-5 h-5" />}
              </button>

              {/* Time Range Select */}
              <select
                value={selectedTimeRange}
                onChange={(e) => setSelectedTimeRange(Number(e.target.value))}
                className={`w-40 px-4 py-2 rounded-md border transition-colors duration-300 ease-in-out ${
                  darkMode
                    ? 'bg-gray-800 border-gray-700 text-gray-100'
                    : 'bg-white border-gray-200 text-gray-800'
                }`}
              >
                {TIME_RANGES.map(({ label, value }) => (
                  <option key={value} value={value}>
                    {label}
                  </option>
                ))}
              </select>
            </div>
          </div>
        </header>

        {/* MAIN CONTENT: Routes */}
        <main className="container mx-auto p-6">
          <Routes>
            <Route
              path="/"
              element={
                <Home darkMode={darkMode} selectedTimeRange={selectedTimeRange} />
              }
            />
            <Route
              path="/cve"
              element={
                <CveMentions darkMode={darkMode} selectedTimeRange={selectedTimeRange} />
              }
            />
            <Route
              path="/science-environment"
              element={
                <ScienceEnvironment
                  darkMode={darkMode}
                  selectedTimeRange={selectedTimeRange}
                />
              }
            />
            <Route
              path="/business-finance-trade"
              element={
                <BusinessFinanceTrade
                  darkMode={darkMode}
                  selectedTimeRange={selectedTimeRange}
                />
              }
            />
            <Route
              path="/ai-ml"
              element={
                <AiMachineLearning
                  darkMode={darkMode}
                  selectedTimeRange={selectedTimeRange}
                />
              }
            />
            <Route
              path="/cybersecurity-privacy"
              element={
                <CybersecurityDataPrivacy
                  darkMode={darkMode}
                  selectedTimeRange={selectedTimeRange}
                />
              }
            />
            <Route
              path="/politics-government"
              element={
                <PoliticsGovernment
                  darkMode={darkMode}
                  selectedTimeRange={selectedTimeRange}
                />
              }
            />
            <Route
              path="/consumer-tech-gadgets"
              element={
                <ConsumerTechGadgets
                  darkMode={darkMode}
                  selectedTimeRange={selectedTimeRange}
                />
              }
            />
            <Route
              path="/automotive-space-transportation"
              element={
                <AutomotiveSpaceTransportation
                  darkMode={darkMode}
                  selectedTimeRange={selectedTimeRange}
                />
              }
            />
            <Route
              path="/enterprise-cloud-computing"
              element={
                <EnterpriseCloudComputing
                  darkMode={darkMode}
                  selectedTimeRange={selectedTimeRange}
                />
              }
            />
            <Route
              path="/other"
              element={
                <Other darkMode={darkMode} selectedTimeRange={selectedTimeRange} />
              }
            />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;