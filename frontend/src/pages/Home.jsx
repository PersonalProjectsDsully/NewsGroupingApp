// src/pages/Home.jsx
import React, { useState, useEffect } from 'react';
// Import Link from react-router-dom
import { Link } from 'react-router-dom';
import { ChevronDown, ChevronRight, Clock, Loader2 } from 'lucide-react';
import Card from '../components/card'; // Assuming Card component exists

// Helper function to map category names to URL paths
const getCategoryPath = (categoryName) => {
  switch (categoryName) {
    case "Science & Environment":
      return "/science-environment";
    case "Business, Finance & Trade":
      return "/business-finance-trade";
    case "Artificial Intelligence & Machine Learning":
      return "/ai-ml";
    case "Cybersecurity & Data Privacy":
      return "/cybersecurity-privacy";
    case "Politics & Government":
      return "/politics-government";
    case "Consumer Technology & Gadgets":
      return "/consumer-tech-gadgets";
    case "Automotive, Space & Transportation":
      return "/automotive-space-transportation";
    case "Enterprise Technology & Cloud Computing":
      return "/enterprise-cloud-computing";
    case "Other":
      return "/other";
    // Add more cases if you have other categories defined in your backend/constants
    default:
      console.warn(`Unknown category name for path mapping: ${categoryName}`);
      return "/"; // Default fallback path (e.g., Home)
  }
};
// --- End helper function ---

export default function Home({ darkMode, selectedTimeRange }) {
  const [homeData, setHomeData] = useState({ categories: [] });
  const [expandedGroups, setExpandedGroups] = useState(new Set());
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true);
        // Fetch data for the home page (top groups per category)
        const res = await fetch(`/api/home_groups?hours=${selectedTimeRange}`);
        if (!res.ok) {
          throw new Error(`HTTP error! status: ${res.status}`);
        }
        const data = await res.json();
        // Ensure data structure is { categories: [ { category: '...', groups: [...] } ] }
        setHomeData(data || { categories: [] }); // Handle potential empty response
      } catch (error) {
        console.error('Error fetching home groups:', error);
        setHomeData({ categories: [] }); // Reset state on error
      } finally {
        setIsLoading(false);
      }
    };
    fetchData();
  }, [selectedTimeRange]); // Re-fetch when the time range changes

  // Function to toggle the expanded state of a group
  const toggleGroup = (id) => {
    setExpandedGroups(prev => {
      const newSet = new Set(prev);
      newSet.has(id) ? newSet.delete(id) : newSet.add(id);
      return newSet;
    });
  };

  // Function to format date strings
  const formatDate = (dateString) => {
    if (!dateString) return '';
    try {
      // Use toLocaleString for user-friendly date/time format based on locale
      return new Date(dateString).toLocaleString();
    } catch (error) {
      console.error("Error formatting date:", error, "Input:", dateString);
      return String(dateString); // Fallback to original string on error
    }
  };

  return (
    <>
      {/* Page Header with Time Range Indicator */}
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-2xl font-semibold">Home Page - Top Groups</h2>
        {selectedTimeRange > 0 && (
          <div className={`flex items-center text-sm py-1 px-3 rounded-full ${darkMode ? 'bg-blue-800 text-blue-100' : 'bg-blue-100 text-blue-800'}`}>
            <Clock className="w-4 h-4 mr-1" />
            Last {selectedTimeRange} hour{selectedTimeRange !== 1 && 's'}
          </div>
        )}
      </div>

      {/* Loading State Indicator */}
      {isLoading && (
          <div className="flex justify-center items-center py-12">
              <Loader2 className={`w-10 h-10 animate-spin ${darkMode ? 'text-blue-400' : 'text-blue-600'}`} />
          </div>
       )}

      {/* No Results Message */}
      {!isLoading && (!homeData.categories || homeData.categories.length === 0) && (
        <div className="text-center py-10">
          <Clock className={`mx-auto w-16 h-16 mb-4 ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}/>
          <h3 className="text-xl font-medium mb-2">No Recent Activity</h3>
          <p className={`max-w-md mx-auto mb-6 ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
            {selectedTimeRange > 0
              ? `No grouped articles found in the last ${selectedTimeRange} hour${selectedTimeRange !== 1 ? 's' : ''}. Check back later or select 'All Time'.`
              : 'No articles have been grouped yet.'}
          </p>
          {/* Optional: Add a button to switch to 'All Time' if no results in selected range */}
        </div>
      )}

      {/* Categories and Groups Loop */}
      {!isLoading && homeData.categories && homeData.categories.length > 0 && homeData.categories.map((cat) => (
        // Outer container for each category section
        <div key={cat.category} className="mb-8">
          {/* Category Header with "View All" Link */}
          <div className="flex justify-between items-center mb-4 border-b pb-2 dark:border-gray-700">
            <h3 className="text-xl font-bold">{cat.category}</h3>
            <Link
              to={getCategoryPath(cat.category)} // Dynamically get the correct path
              className={`text-sm font-medium transition-colors duration-200 ${
                darkMode
                  ? 'text-blue-400 hover:text-blue-300'
                  : 'text-blue-600 hover:text-blue-800'
              } hover:underline`}
            >
              View All → {/* Right arrow entity for visual cue */}
            </Link>
          </div>

          {/* Grid for displaying the top 3 groups within the category */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {/* Map through the groups for the current category */}
            {cat.groups && cat.groups.map((group) => (
              // Card component for each group
              <Card
                key={group.group_id} // Unique key for each group card
                className={`border rounded-md shadow-sm transition-colors duration-300 ease-in-out ${darkMode ? 'bg-gray-800 border-gray-700 text-gray-300' : 'bg-white border-gray-200 text-gray-800'}`}
              >
                {/* Card Header: Group Label and Toggle Button */}
                <div className="p-4">
                  <h4 className="font-semibold">
                    <button
                      onClick={() => toggleGroup(group.group_id)} // Toggle article list visibility
                      className="w-full text-left flex justify-between items-center"
                    >
                      {/* Group Label and Article Count */}
                      <span>
                        {group.group_label} (
                        <span className={`font-normal ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                          {group.article_count}
                        </span>
                        )
                      </span>
                      {/* Chevron icon indicating expanded/collapsed state */}
                      {expandedGroups.has(group.group_id) ? <ChevronDown className="w-5 h-5" /> : <ChevronRight className="w-5 h-5" />}
                    </button>
                  </h4>
                </div>

                {/* Conditionally Rendered Card Body: Group Description and Article List */}
                {expandedGroups.has(group.group_id) && (
                  <div className="p-4 pt-0">
                    {/* Group Description */}
                    <p className="text-sm mb-4">{group.description}</p>
                    {/* List of Articles */}
                    <ul className="space-y-2">
                      {/* Map through articles within the group */}
                      {group.articles && group.articles.map((a) => (
                        <li key={a.article_id || a.link} className="border-t pt-2 dark:border-gray-700"> {/* Added dark mode border */}
                          {/* Link to the article */}
                          <a
                            href={a.link}
                            className={`text-sm underline-offset-2 hover:underline block ${darkMode ? 'text-blue-400' : 'text-blue-500'}`}
                            target="_blank" // Open in new tab
                            rel="noopener noreferrer" // Security best practice
                          >
                            {a.title} {/* Article Title */}
                          </a>
                          {/* Article Publish Date */}
                          <div className={`text-xs mt-1 ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                            {formatDate(a.published_date)}
                          </div>
                        </li>
                      ))}
                      {/* Handle case where group has count but no articles (e.g., filtering issue) */}
                      {(!group.articles || group.articles.length === 0) && (
                         <li className={`text-xs italic ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>No articles found for this time range.</li>
                      )}
                    </ul>
                  </div>
                )}
              </Card>
            ))}
            {/* Handle case where category exists but has no groups */}
             {(!cat.groups || cat.groups.length === 0) && (
                <div className={`col-span-full text-center text-sm italic py-4 ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
                    No groups found in this category for the selected time range.
                </div>
             )}
          </div>
        </div>
      ))}
    </>
  );
}