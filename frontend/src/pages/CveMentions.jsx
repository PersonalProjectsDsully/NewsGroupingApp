// src/pages/CveMentions.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { Clock, ShieldAlert, Loader2, ArrowUpDown, ArrowUp, ArrowDown } from 'lucide-react';

// Helper function to get nested property safely
const getNestedValue = (obj, path) => {
  return path.split('.').reduce((acc, part) => acc && acc[part], obj);
};

// Custom hook for sorting logic
const useSortableData = (items, initialConfig = null) => {
  const [sortConfig, setSortConfig] = useState(initialConfig);

  const sortedItems = useMemo(() => {
    if (!items) {
      return [];
    }
    let sortableItems = [...items];
    if (sortConfig !== null) {
      sortableItems.sort((a, b) => {
        const aValue = getNestedValue(a, sortConfig.key);
        const bValue = getNestedValue(b, sortConfig.key);

        if (aValue === null || aValue === undefined) return 1;
        if (bValue === null || bValue === undefined) return -1;

        let comparison = 0;
        if (typeof aValue === 'number' && typeof bValue === 'number') {
          comparison = aValue - bValue;
        } else if (aValue instanceof Date && bValue instanceof Date) {
          comparison = aValue.getTime() - bValue.getTime();
        } else {
          comparison = String(aValue).localeCompare(String(bValue), undefined, { numeric: true, sensitivity: 'base' });
        }

        return sortConfig.direction === 'ascending' ? comparison : comparison * -1;
      });
    }
    return sortableItems;
  }, [items, sortConfig]);

  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig && sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  return { items: sortedItems, requestSort, sortConfig };
};
// --- End custom hook ---

export default function CveMentions({ darkMode, selectedTimeRange }) {
  const [rawCveData, setRawCveData] = useState([]);
  const [isLoading, setIsLoading] = useState(false);

  // Use the sorting hook with initial sort on 'times_seen' descending
  const { items: sortedCveData, requestSort, sortConfig } = useSortableData(
    rawCveData,
    { key: 'times_seen', direction: 'descending' }
  );

  // Function to convert UTC date to Eastern Time
  const convertToEasternTime = (utcDate) => {
    if (!utcDate) return '';
    try {
      const date = new Date(utcDate);
      return new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        year: 'numeric', month: 'short', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true
      }).format(date) + ' EST';
    } catch (error) {
      console.error("Error formatting date:", error, "Input:", utcDate);
      return utcDate instanceof Date ? utcDate.toISOString() : String(utcDate);
    }
  };

  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true);
        const res = await fetch(`/api/cve_table?hours=${selectedTimeRange}`);
        if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
        const data = await res.json();

        const processedData = data.map((row) => ({
          ...row,
          first_mention: row.first_mention ? new Date(row.first_mention) : null,
          last_mention: row.last_mention ? new Date(row.last_mention) : null,
          article_links: Array.isArray(row.article_links) ? row.article_links : [],
          base_score: row.base_score !== null && !isNaN(parseFloat(row.base_score)) ? parseFloat(row.base_score) : null,
        }));

        setRawCveData(processedData);
      } catch (error) {
        console.error('Error fetching CVE table:', error);
        setRawCveData([]);
      } finally {
        setIsLoading(false);
      }
    };
    fetchData();
  }, [selectedTimeRange]);

  // Helper function to render sort icons
  const getSortIcon = (columnKey) => {
    if (!sortConfig || sortConfig.key !== columnKey) {
      return <ArrowUpDown className="w-3 h-3 ml-1 opacity-40" />;
    }
    if (sortConfig.direction === 'ascending') {
      return <ArrowUp className="w-3 h-3 ml-1" />;
    }
    return <ArrowDown className="w-3 h-3 ml-1" />;
  };

  return (
    <>
      <div className="flex justify-between items-center mb-6">
         <h2 className="text-2xl font-semibold flex items-center">
           <ShieldAlert className="w-6 h-6 mr-2" />
           CVE Mentions & Analysis
         </h2>
         {selectedTimeRange > 0 && (
           <div
             className={`flex items-center text-sm py-1 px-3 rounded-full ${
               darkMode ? 'bg-blue-800 text-blue-100' : 'bg-blue-100 text-blue-800'
             }`}
           >
             <Clock className="w-4 h-4 mr-1" />
             Last {selectedTimeRange} hour{selectedTimeRange !== 1 && 's'}
           </div>
         )}
       </div>

       {isLoading && (
          <div className="flex justify-center items-center py-12">
           <Loader2
             className={`w-10 h-10 animate-spin ${
               darkMode ? 'text-blue-400' : 'text-blue-600'
             }`}
           />
         </div>
       )}

       {!isLoading && rawCveData.length === 0 && (
          <div className="text-center py-10">
           <div
             className={`mx-auto w-16 h-16 rounded-full flex items-center justify-center mb-4 ${
               darkMode ? 'bg-gray-800' : 'bg-gray-100'
             }`}
           >
             <ShieldAlert
               className={`w-8 h-8 ${
                 darkMode ? 'text-gray-300' : 'text-gray-500'
               }`}
             />
           </div>
           <h3 className="text-xl font-medium mb-2">No CVE mentions found</h3>
           <p
             className={`max-w-md mx-auto mb-6 ${
               darkMode ? 'text-gray-400' : 'text-gray-500'
             }`}
           >
             {selectedTimeRange > 0
               ? `No CVE mentions were found in the last ${selectedTimeRange} hour${
                   selectedTimeRange !== 1 ? 's' : ''
                 }.`
               : 'No CVE mentions were found.'}
           </p>
           {selectedTimeRange > 0 && (
             <button
               onClick={() => window.location.reload()}
               className={`px-4 py-2 rounded ${
                 darkMode
                   ? 'bg-blue-600 hover:bg-blue-700 text-white'
                   : 'bg-blue-500 hover:bg-blue-600 text-white'
               }`}
             >
               Refresh Page
             </button>
           )}
         </div>
       )}

      {/* Results table */}
      {!isLoading && sortedCveData.length > 0 && (
        <div
          className={`overflow-x-auto rounded-lg border ${
            darkMode ? 'border-gray-700' : 'border-gray-200'
          }`}
        >
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className={`${darkMode ? 'bg-gray-800' : 'bg-gray-100'}`}>
                <th className="p-3 text-left font-medium cursor-pointer hover:bg-opacity-75" onClick={() => requestSort('cve_id')}>
                  <div className="flex items-center">CVE ID {getSortIcon('cve_id')}</div>
                </th>
                <th className="p-3 text-left font-medium cursor-pointer hover:bg-opacity-75" onClick={() => requestSort('times_seen')}>
                  <div className="flex items-center">Times Seen {getSortIcon('times_seen')}</div>
                </th>
                <th className="p-3 text-left font-medium cursor-pointer hover:bg-opacity-75" onClick={() => requestSort('first_mention')}>
                  <div className="flex items-center">First Mention (EST) {getSortIcon('first_mention')}</div>
                </th>
                <th className="p-3 text-left font-medium cursor-pointer hover:bg-opacity-75" onClick={() => requestSort('last_mention')}>
                   <div className="flex items-center">Last Mention (EST) {getSortIcon('last_mention')}</div>
                </th>
                <th className="p-3 text-left font-medium">Articles</th>
                <th className="p-3 text-left font-medium cursor-pointer hover:bg-opacity-75" onClick={() => requestSort('base_score')}>
                   <div className="flex items-center">Base Score {getSortIcon('base_score')}</div>
                </th>
                <th className="p-3 text-left font-medium cursor-pointer hover:bg-opacity-75" onClick={() => requestSort('vendor')}>
                   <div className="flex items-center">Vendor {getSortIcon('vendor')}</div>
                </th>
                <th className="p-3 text-left font-medium">Affected Products</th>
                <th className="p-3 text-left font-medium">MITRE Link</th>
                <th className="p-3 text-left font-medium">Vendor Link</th>
                <th className="p-3 text-left font-medium">Solution</th>
              </tr>
            </thead>
            <tbody
              className={`divide-y ${
                darkMode ? 'divide-gray-700' : 'divide-gray-200'
              }`}
            >
              {sortedCveData.map((row, i) => {
                 const sourceCounts = {};
                 return (
                    <tr
                      key={row.cve_id || i}
                      className={`${
                        darkMode
                          ? 'hover:bg-gray-700 border-gray-700'
                          : 'hover:bg-gray-50 border-gray-200'
                      }`}
                    >
                      {/* CVE ID */}
                      <td className="p-3">
                        {row.cve_id ? (
                           <a
                            href={row.cve_page_link}
                            target="_blank"
                            rel="noopener noreferrer"
                            className={`${
                              darkMode
                                ? 'text-blue-400 hover:text-blue-300'
                                : 'text-blue-600 hover:text-blue-800'
                            } font-medium hover:underline`}
                          >
                            {row.cve_id}
                          </a>
                        ) : 'N/A'}
                      </td>
                      {/* Times Seen */}
                      <td className="p-3">{row.times_seen ?? ''}</td>
                      {/* First Mention */}
                      <td className="p-3">{convertToEasternTime(row.first_mention)}</td>
                      {/* Last Mention */}
                      <td className="p-3">{convertToEasternTime(row.last_mention)}</td>
                      {/* Articles */}
                      <td className="p-3">
                        {row.article_links && row.article_links.length > 0 ? (
                          row.article_links.map((linkObj, idx) => {
                            const source = linkObj.source || 'unknown';
                            sourceCounts[source] = (sourceCounts[source] || 0) + 1;
                            const label = `${source} ${sourceCounts[source]}`;
                            return (
                              <div key={`${linkObj.url}-${idx}`}>
                                <a
                                  href={linkObj.url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className={`underline-offset-2 hover:underline block ${
                                    darkMode
                                      ? 'text-blue-400 hover:text-blue-300'
                                      : 'text-blue-600 hover:text-blue-800'
                                  } text-xs`}
                                >
                                  {label}
                                </a>
                              </div>
                            );
                          })
                        ) : (
                          <span className={`text-xs ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>None</span>
                        )}
                      </td>
                      {/* Base Score */}
                      <td className="p-3">
                        {row.base_score !== null && (
                          <span
                            className={`px-2 py-1 rounded text-xs ${
                              row.base_score >= 8
                                ? darkMode ? 'bg-red-900 text-red-200' : 'bg-red-100 text-red-800'
                                : row.base_score >= 5
                                ? darkMode ? 'bg-yellow-900 text-yellow-200' : 'bg-yellow-100 text-yellow-800'
                                : darkMode ? 'bg-green-900 text-green-200' : 'bg-green-100 text-green-800'
                            }`}
                          >
                            {row.base_score.toFixed(1)}
                          </span>
                        )}
                      </td>
                      {/* Vendor */}
                      <td className="p-3">{row.vendor ?? ''}</td>
                      {/* Affected Products */}
                      <td className="p-3">{row.affected_products ?? ''}</td>
                      {/* MITRE Link */}
                      <td className="p-3">
                        {row.cve_id && row.cve_page_link && (
                           <a
                            href={row.cve_page_link}
                            target="_blank"
                            rel="noopener noreferrer"
                            className={`${
                              darkMode
                                ? 'text-blue-400 hover:text-blue-300'
                                : 'text-blue-600 hover:text-blue-800'
                            } hover:underline`}
                          >
                            MITRE
                          </a>
                        )}
                      </td>
                      {/* Vendor Link */}
                      <td className="p-3">
                        {row.vendor_link && (
                           <a
                            href={row.vendor_link}
                            target="_blank"
                            rel="noopener noreferrer"
                            className={`${
                              darkMode
                                ? 'text-blue-400 hover:text-blue-300'
                                : 'text-blue-600 hover:text-blue-800'
                            } hover:underline`}
                          >
                            Vendor
                          </a>
                        )}
                      </td>
                      {/* Solution */}
                      <td className="p-3 max-w-xs truncate">{row.solution ?? ''}</td>
                    </tr>
                 );
              })}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}