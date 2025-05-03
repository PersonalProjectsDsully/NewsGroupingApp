// src/pages/EnterpriseCloudComputing.jsx
import React, { useState, useEffect } from 'react';
import { ChevronDown, ChevronRight, Clock, Loader2, Server } from 'lucide-react';
import Card from '../components/card'; // Assuming Card component exists

export default function EnterpriseCloudComputing({ darkMode, selectedTimeRange }) {
  // Renamed state variables
  const [groups, setGroups] = useState([]);
  const [expandedGroups, setExpandedGroups] = useState(new Set());
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true);
        // *** UPDATED API ENDPOINT ***
        const res = await fetch(`/api/enterprise_cloud_computing_groups?hours=${selectedTimeRange}`);
        if (!res.ok) {
          throw new Error(`HTTP error! status: ${res.status}`);
        }
        const data = await res.json();
        // *** Expect 'groups' key ***
        // Sort groups by recent article count
        const sorted = (data.groups || []).sort(
          (a, b) => b.article_count - a.article_count
        );
        setGroups(sorted); // Use renamed setter
      } catch (error) {
        console.error('Error fetching Enterprise/Cloud groups:', error);
        setGroups([]); // Use renamed setter
      } finally {
        setIsLoading(false);
      }
    };
    fetchData();
  }, [selectedTimeRange]);

  // Renamed toggle function
  const toggleGroup = (id) => {
    setExpandedGroups((prev) => { // Use renamed setter
      const newSet = new Set(prev);
      newSet.has(id) ? newSet.delete(id) : newSet.add(id);
      return newSet;
    });
  };

  const formatDate = (dateString) => {
    if (!dateString) return '';
    try {
      return new Date(dateString).toLocaleString();
    } catch (error) {
      return dateString;
    }
  };

  return (
    <>
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-2xl font-semibold flex items-center"> {/* Added flex */}
          <Server className="inline-block mr-2 h-6 w-6" />
          Enterprise Technology & Cloud Computing
        </h2>
        {selectedTimeRange > 0 && (
          <div className={`flex items-center text-sm py-1 px-3 rounded-full ${
            darkMode ? 'bg-blue-800 text-blue-100' : 'bg-blue-100 text-blue-800'
          }`}>
            <Clock className="w-4 h-4 mr-1" />
            Last {selectedTimeRange} hour{selectedTimeRange !== 1 && 's'}
          </div>
        )}
      </div>

      {isLoading && (
        <div className="flex justify-center items-center py-12">
          <Loader2 className={`w-10 h-10 animate-spin ${darkMode ? 'text-blue-400' : 'text-blue-600'}`} />
        </div>
      )}

      {/* *** Check groups.length *** */}
      {!isLoading && groups.length === 0 && (
        <div className="text-center py-10">
          <div className={`mx-auto w-16 h-16 rounded-full flex items-center justify-center mb-4 ${darkMode ? 'bg-gray-800' : 'bg-gray-100'}`}>
            <Server className={`w-8 h-8 ${darkMode ? 'text-gray-300' : 'text-gray-500'}`} />
          </div>
          <h3 className="text-xl font-medium mb-2">No enterprise tech updates</h3>
          <p className={`text-gray-500 dark:text-gray-400 max-w-md mx-auto mb-6`}>
            {selectedTimeRange > 0
              ? `No enterprise tech articles found in the last ${selectedTimeRange} hour${selectedTimeRange !== 1 ? 's' : ''}.`
              : 'No enterprise tech articles found.'}
          </p>
          {selectedTimeRange > 0 && (
            <button
              onClick={() => window.location.reload()}
              className={`px-4 py-2 rounded ${darkMode ? 'bg-blue-600 hover:bg-blue-700 text-white' : 'bg-blue-500 hover:bg-blue-600 text-white'}`}
            >
              Refresh Page
            </button>
          )}
        </div>
      )}

      {/* *** Check and map groups *** */}
      {!isLoading && groups.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
           {/* *** Map over 'groups' and use 'group' *** */}
          {groups.map((group) => (
            <Card
              key={group.group_id} // Use group_id
              className={`border rounded-md shadow-sm ${darkMode ? 'bg-gray-800 border-gray-700 text-gray-300' : 'bg-white border-gray-200 text-gray-800'}`}
            >
              <div className="p-4">
                <h3 className="font-semibold">
                  {/* *** Use toggleGroup and group.group_id *** */}
                  <button
                    onClick={() => toggleGroup(group.group_id)}
                    className="w-full text-left flex justify-between items-center"
                  >
                    <span>
                      {group.group_label} ( {/* Use group_label */}
                      <span className={`font-normal ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                        {group.article_count} {/* Use article_count */}
                      </span>
                      )
                    </span>
                     {/* *** Use expandedGroups and group.group_id *** */}
                    {expandedGroups.has(group.group_id) ? <ChevronDown className="w-5 h-5" /> : <ChevronRight className="w-5 h-5" />}
                  </button>
                </h3>
              </div>
              {/* *** Use expandedGroups and group.group_id *** */}
              {expandedGroups.has(group.group_id) && (
                <div className="p-4 pt-0">
                   {/* *** Use group.description *** */}
                  <p className="text-sm mb-4">{group.description}</p>
                  <ul className="space-y-2">
                    {/* *** Loop through group.articles *** */}
                    {group.articles.map((a) => (
                      <li key={a.article_id || a.link} className="border-t pt-2 dark:border-gray-700"> {/* Added dark mode border */}
                        <a
                          href={a.link}
                          className={`text-sm underline-offset-2 hover:underline block ${darkMode ? 'text-blue-400' : 'text-blue-500'}`}
                          target="_blank"
                          rel="noopener noreferrer"
                        >
                          {a.title}
                        </a>
                        <div className={`text-xs mt-1 ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                          {formatDate(a.published_date)}
                        </div>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </Card>
          ))}
        </div>
      )}
    </>
  );
}