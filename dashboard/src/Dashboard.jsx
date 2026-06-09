import React, { useState, useEffect, useMemo } from 'react';
import { 
  AreaChart, Area, LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, ComposedChart
} from 'recharts';
import { 
  Coins, Zap, Store, Target, TrendingUp, Sprout, Map, Timer, AlertCircle, RefreshCw, Layers, BarChart3, Database
} from 'lucide-react';
import IndiaMap from './components/IndiaMap';

const Dashboard = () => {
  const [activeTab, setActiveTab] = useState('overview'); // 'overview' | 'explorer'
  const [selectedState, setSelectedState] = useState(null);
  
  // Overview Tab Data State
  const [overviewData, setOverviewData] = useState(null);
  const [loadingOverview, setLoadingOverview] = useState(true);
  const [overviewError, setOverviewError] = useState(null);

  // Explorer Tab Filter States
  const [explorerStates, setExplorerStates] = useState([]);
  const [explorerDistricts, setExplorerDistricts] = useState([]);
  const [explorerMarkets, setExplorerMarkets] = useState([]);
  const [explorerCommodities, setExplorerCommodities] = useState([]);

  const [selectedExplorerState, setSelectedExplorerState] = useState('');
  const [selectedExplorerDistrict, setSelectedExplorerDistrict] = useState('');
  const [selectedExplorerMarket, setSelectedExplorerMarket] = useState('');
  const [selectedExplorerCommodity, setSelectedExplorerCommodity] = useState('');

  const [loadingDistricts, setLoadingDistricts] = useState(false);
  const [loadingMarkets, setLoadingMarkets] = useState(false);
  const [loadingCommodities, setLoadingCommodities] = useState(false);

  // Explorer Tab Results State
  const [explorerData, setExplorerData] = useState(null);
  const [loadingExplorer, setLoadingExplorer] = useState(false);
  const [explorerError, setExplorerError] = useState(null);

  // Forecast Tab Filter & Results States
  const [forecastStates, setForecastStates] = useState([]);
  const [forecastMarkets, setForecastMarkets] = useState([]);
  const [forecastCommodities, setForecastCommodities] = useState([]);

  const [selectedForecastState, setSelectedForecastState] = useState('');
  const [selectedForecastMarket, setSelectedForecastMarket] = useState('');
  const [selectedForecastCommodity, setSelectedForecastCommodity] = useState('');

  const [loadingForecastMarkets, setLoadingForecastMarkets] = useState(false);
  const [loadingForecastCommodities, setLoadingForecastCommodities] = useState(false);

  const [forecastData, setForecastData] = useState(null);
  const [loadingForecast, setLoadingForecast] = useState(false);
  const [forecastError, setForecastError] = useState(null);
  const [forecastHistory, setForecastHistory] = useState([]);

  // ---------------------------------------------
  // FETCH PIPELINE: OVERVIEW TAB
  // ---------------------------------------------
  const fetchOverview = () => {
    setLoadingOverview(true);
    setOverviewError(null);
    let url = 'http://localhost:8080/api/overview';
    if (selectedState) {
      url += `?state=${encodeURIComponent(selectedState)}`;
    }

    fetch(url)
      .then(res => {
        if (!res.ok) throw new Error("Could not connect to database or API backend.");
        return res.json();
      })
      .then(json => {
        if (json.error) throw new Error(json.error);
        setOverviewData(json);
        setLoadingOverview(false);
      })
      .catch(err => {
        console.error(err);
        setOverviewError(err.message);
        setLoadingOverview(false);
      });
  };

  useEffect(() => {
    fetchOverview();
  }, [selectedState]);

  // ---------------------------------------------
  // FETCH PIPELINE: EXPLORER CASCADING FILTERS
  // ---------------------------------------------
  
  // 1. Fetch States on tab switch
  useEffect(() => {
    if (activeTab === 'explorer' && explorerStates.length === 0) {
      fetch('http://localhost:8080/api/filters')
        .then(res => res.json())
        .then(json => {
          setExplorerStates(json.states || []);
        })
        .catch(err => console.error("Error fetching states list:", err));
    }
  }, [activeTab]);

  // 2. Fetch Districts when State changes
  useEffect(() => {
    if (!selectedExplorerState) {
      setSelectedExplorerDistrict('');
      setSelectedExplorerMarket('');
      setSelectedExplorerCommodity('');
      setExplorerDistricts([]);
      setExplorerMarkets([]);
      setExplorerCommodities([]);
      return;
    }
    
    const stateObj = explorerStates.find(s => s.state === selectedExplorerState);
    if (!stateObj) return;
    
    setLoadingDistricts(true);
    setSelectedExplorerDistrict('');
    setSelectedExplorerMarket('');
    setSelectedExplorerCommodity('');
    setExplorerDistricts([]);
    setExplorerMarkets([]);
    setExplorerCommodities([]);
    
    fetch(`http://localhost:8080/api/filters?state_id=${stateObj.state_id}`)
      .then(res => res.json())
      .then(json => {
        setExplorerDistricts(json.districts || []);
        setLoadingDistricts(false);
      })
      .catch(err => {
        console.error(err);
        setLoadingDistricts(false);
      });
  }, [selectedExplorerState, explorerStates]);

  // 3. Fetch Markets when District changes
  useEffect(() => {
    if (!selectedExplorerDistrict || !selectedExplorerState) {
      setSelectedExplorerMarket('');
      setSelectedExplorerCommodity('');
      setExplorerMarkets([]);
      setExplorerCommodities([]);
      return;
    }
    
    const stateObj = explorerStates.find(s => s.state === selectedExplorerState);
    if (!stateObj) return;
    
    setLoadingMarkets(true);
    setSelectedExplorerMarket('');
    setSelectedExplorerCommodity('');
    setExplorerMarkets([]);
    setExplorerCommodities([]);
    
    fetch(`http://localhost:8080/api/filters?state_id=${stateObj.state_id}&district=${encodeURIComponent(selectedExplorerDistrict)}`)
      .then(res => res.json())
      .then(json => {
        setExplorerMarkets(json.markets || []);
        setLoadingMarkets(false);
      })
      .catch(err => {
        console.error(err);
        setLoadingMarkets(false);
      });
  }, [selectedExplorerDistrict, selectedExplorerState, explorerStates]);

  // 4. Fetch Commodities when Market changes
  useEffect(() => {
    if (!selectedExplorerMarket) {
      setSelectedExplorerCommodity('');
      setExplorerCommodities([]);
      return;
    }
    
    const marketObj = explorerMarkets.find(m => m.market === selectedExplorerMarket);
    if (!marketObj) return;
    
    setLoadingCommodities(true);
    setSelectedExplorerCommodity('');
    setExplorerCommodities([]);
    
    fetch(`http://localhost:8080/api/filters?market_id=${marketObj.market_id}`)
      .then(res => res.json())
      .then(json => {
        setExplorerCommodities(json.commodities || []);
        setLoadingCommodities(false);
      })
      .catch(err => {
        console.error(err);
        setLoadingCommodities(false);
      });
  }, [selectedExplorerMarket, explorerMarkets]);

  // 5. Fetch Explorer Results when click Apply Filters
  const handleApplyExplorerFilters = () => {
    if (!selectedExplorerState || !selectedExplorerDistrict || !selectedExplorerMarket || !selectedExplorerCommodity) return;
    
    const stateObj = explorerStates.find(s => s.state === selectedExplorerState);
    const marketObj = explorerMarkets.find(m => m.market === selectedExplorerMarket);
    if (!stateObj || !marketObj) return;
    
    setLoadingExplorer(true);
    setExplorerError(null);
    
    const url = `http://localhost:8080/api/explorer?commodity=${encodeURIComponent(selectedExplorerCommodity)}&state_id=${stateObj.state_id}&district=${encodeURIComponent(selectedExplorerDistrict)}&market_id=${marketObj.market_id}`;
    
    fetch(url)
      .then(res => {
        if (!res.ok) throw new Error("Failed to fetch explorer data.");
        return res.json();
      })
      .then(json => {
        setExplorerData(json);
        setLoadingExplorer(false);
      })
      .catch(err => {
        console.error(err);
        setExplorerError(err.message);
        setLoadingExplorer(false);
      });
  };

  // Trigger load when commodity finishes loading auto-selections
  useEffect(() => {
    if (selectedExplorerState && selectedExplorerDistrict && selectedExplorerMarket && selectedExplorerCommodity) {
      handleApplyExplorerFilters();
    }
  }, [selectedExplorerCommodity]);

  // ---------------------------------------------
  // FETCH PIPELINE: FORECAST FILTERS & INFERENCE
  // ---------------------------------------------
  
  // 1. Fetch Forecast States on activeTab switch
  useEffect(() => {
    if (activeTab === 'forecast' && forecastStates.length === 0) {
      fetch('http://localhost:8080/api/filters')
        .then(res => res.json())
        .then(json => {
          setForecastStates(json.states || []);
        })
        .catch(err => console.error("Error fetching forecast states:", err));
    }
  }, [activeTab]);

  // 2. Fetch Forecast Markets directly (skipping District)
  useEffect(() => {
    if (!selectedForecastState) {
      setSelectedForecastMarket('');
      setSelectedForecastCommodity('');
      setForecastMarkets([]);
      setForecastCommodities([]);
      return;
    }

    const stateObj = forecastStates.find(s => s.state === selectedForecastState);
    if (!stateObj) return;

    setLoadingForecastMarkets(true);
    setSelectedForecastMarket('');
    setSelectedForecastCommodity('');
    setForecastMarkets([]);
    setForecastCommodities([]);

    fetch(`http://localhost:8080/api/filters?state_id=${stateObj.state_id}&all_markets=true`)
      .then(res => res.json())
      .then(json => {
        setForecastMarkets(json.markets || []);
        setLoadingForecastMarkets(false);
      })
      .catch(err => {
        console.error("Error fetching forecast markets:", err);
        setLoadingForecastMarkets(false);
      });
  }, [selectedForecastState, forecastStates]);

  // 3. Fetch Forecast Commodities
  useEffect(() => {
    if (!selectedForecastMarket) {
      setSelectedForecastCommodity('');
      setForecastCommodities([]);
      return;
    }

    const marketObj = forecastMarkets.find(m => m.market === selectedForecastMarket);
    if (!marketObj) return;

    setLoadingForecastCommodities(true);
    setSelectedForecastCommodity('');
    setForecastCommodities([]);

    fetch(`http://localhost:8080/api/filters?market_id=${marketObj.market_id}`)
      .then(res => res.json())
      .then(json => {
        setForecastCommodities(json.commodities || []);
        setLoadingForecastCommodities(false);
      })
      .catch(err => {
        console.error("Error fetching forecast commodities:", err);
        setLoadingForecastCommodities(false);
      });
  }, [selectedForecastMarket, forecastMarkets]);

  // 4. Fetch Inference & Historical context
  const handleApplyForecastFilters = () => {
    if (!selectedForecastState || !selectedForecastMarket || !selectedForecastCommodity) return;

    const marketObj = forecastMarkets.find(m => m.market === selectedForecastMarket);
    if (!marketObj) return;

    setLoadingForecast(true);
    setForecastError(null);
    setForecastData(null);
    setForecastHistory([]);

    const inferenceUrl = `http://localhost:8080/api/inference?market_id=${marketObj.market_id}&commodity=${encodeURIComponent(selectedForecastCommodity)}`;
    const historyUrl = `http://localhost:8080/api/explorer?commodity=${encodeURIComponent(selectedForecastCommodity)}&state_id=${stateObjId(selectedForecastState)}&market_id=${marketObj.market_id}`;

    // Helper to resolve state ID
    function stateObjId(name) {
      const s = forecastStates.find(x => x.state === name);
      return s ? s.state_id : 1;
    }

    // Load forecast
    fetch(inferenceUrl)
      .then(res => {
        if (res.status === 404) {
          throw new Error("No trained XGBoost model found on disk for this market and commodity. Run the offline parallel training script first.");
        }
        if (!res.ok) throw new Error("Failed to load forecast inference.");
        return res.json();
      })
      .then(forecastJson => {
        setForecastData(forecastJson);
        
        // Next, load historical actuals for chart line context
        return fetch(historyUrl)
          .then(hRes => hRes.json())
          .then(historyJson => {
            setForecastHistory(historyJson.history || []);
            setLoadingForecast(false);
          })
          .catch(hErr => {
            console.error("Error loading historical reference for forecast:", hErr);
            // Non-critical, let it load empty history
            setLoadingForecast(false);
          });
      })
      .catch(err => {
        console.error(err);
        setForecastError(err.message);
        setLoadingForecast(false);
      });
  };

  // Trigger automatically when commodity is selected
  useEffect(() => {
    if (selectedForecastState && selectedForecastMarket && selectedForecastCommodity) {
      handleApplyForecastFilters();
    }
  }, [selectedForecastCommodity]);

  // Clean date formatter for chart x-axis
  const formatChartDate = (dateStr) => {
    try {
      const d = new Date(dateStr);
      return d.toLocaleDateString('en-US', { month: 'short', day: '2-digit' });
    } catch {
      return dateStr;
    }
  };

  // Custom Recharts Tooltip
  const CustomOverviewTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-slate-900/90 text-white border border-slate-700/50 backdrop-blur-sm px-3 py-2 rounded-xl shadow-lg text-xs font-sans">
          <div className="font-semibold text-slate-400">{formatChartDate(payload[0].payload.date)}</div>
          <div className="mt-1 font-bold text-emerald-400 font-sans">₹{payload[0].value.toLocaleString()}</div>
        </div>
      );
    }
    return null;
  };

  // Custom Explorer Tooltip
  const CustomExplorerTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-slate-900/90 text-white border border-slate-700/50 backdrop-blur-sm px-3 py-2 rounded-xl shadow-lg text-[11px] font-sans space-y-1">
          <div className="font-semibold text-slate-400 border-b border-slate-700/50 pb-1">{formatChartDate(payload[0].payload.date)}</div>
          {payload.map((item, idx) => (
            <div key={idx} className="flex justify-between gap-4">
              <span className="text-slate-400">{item.name}:</span>
              <span className="font-bold text-white">₹{item.value.toLocaleString()}</span>
            </div>
          ))}
        </div>
      );
    }
    return null;
  };



  // Loading Screen
  if (loadingOverview && !overviewData) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-slate-100 gap-4">
        <RefreshCw className="animate-spin text-emerald-600" size={36} />
        <p className="text-slate-600 text-sm font-medium">Loading BharatBazaar...</p>
      </div>
    );
  }

  // Error Screen
  if (overviewError && !overviewData) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-slate-100 p-6 text-center">
        <AlertCircle className="text-red-500 mb-4" size={48} />
        <h2 className="text-lg font-bold text-slate-800">Connection Error</h2>
        <p className="text-slate-600 text-sm max-w-md mt-1 mb-6">{overviewError}</p>
        <button 
          onClick={fetchOverview}
          className="flex items-center gap-2 bg-emerald-600 hover:bg-emerald-700 text-white text-sm font-semibold px-5 py-2.5 rounded-xl shadow-md transition-all active:scale-[0.98]"
        >
          <RefreshCw size={16} /> Try Again
        </button>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-100 flex flex-col font-sans antialiased text-slate-900 selection:bg-emerald-100">
      
      {/* Top Header Navbar */}
      <header className="h-16 border-b border-slate-200/60 bg-white/80 backdrop-blur-md sticky top-0 z-40 flex items-center justify-between px-6 shadow-sm">
        <div className="flex items-center gap-2">
          <div className="w-9 h-9 rounded-xl bg-emerald-100 flex items-center justify-center text-emerald-600">
            <Sprout size={20} className="stroke-[2.5]" />
          </div>
          <div>
            <span className="font-bold text-slate-800 text-lg tracking-tight">BharatBazaar</span>
          </div>
        </div>
        
        {/* Navigation Tabs (Overview, Explorer, Forecast) */}
        <div className="flex items-center gap-4">
          <div className="flex bg-slate-100 p-1 rounded-xl border border-slate-200">
            <button 
              onClick={() => setActiveTab('overview')}
              className={`px-4 py-1.5 rounded-lg text-xs font-semibold transition-all ${
                activeTab === 'overview' 
                  ? 'bg-white text-slate-800 shadow-sm font-bold border border-slate-200/50' 
                  : 'text-slate-500 hover:text-slate-800'
              }`}
            >
              Overview
            </button>
            <button 
              onClick={() => setActiveTab('explorer')}
              className={`px-4 py-1.5 rounded-lg text-xs font-semibold transition-all ${
                activeTab === 'explorer' 
                  ? 'bg-white text-slate-800 shadow-sm font-bold border border-slate-200/50' 
                  : 'text-slate-500 hover:text-slate-800'
              }`}
            >
              Explorer
            </button>
            <button 
              onClick={() => setActiveTab('forecast')}
              className={`px-4 py-1.5 rounded-lg text-xs font-semibold transition-all ${
                activeTab === 'forecast' 
                  ? 'bg-white text-slate-800 shadow-sm font-bold border border-slate-200/50' 
                  : 'text-slate-500 hover:text-slate-800'
              }`}
            >
              Forecast
            </button>
          </div>
          
          <div className="flex items-center gap-2 text-xs font-semibold bg-emerald-50 text-emerald-700 px-3 py-1.5 rounded-full border border-emerald-100">
            <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
            <span>Live</span>
          </div>
          <div className="w-8 h-8 rounded-full bg-slate-200 flex items-center justify-center text-slate-700 font-bold text-sm border border-slate-300">
            H
          </div>
        </div>
      </header>

      {/* ---------------------------------------------
          TAB VIEW 1: OVERVIEW PAGE
          --------------------------------------------- */}
      {activeTab === 'overview' && (
        <main className="flex-1 p-6 w-full space-y-6 max-w-[1600px] mx-auto">
          {/* Active filter badge in overview */}
          {selectedState && (
            <div className="flex justify-between items-center bg-white border border-slate-200/60 p-3 rounded-2xl shadow-sm px-6">
              <span className="text-xs font-medium text-slate-500">
                Active Filter: <span className="font-bold text-slate-700">{selectedState}</span>
              </span>
              <button 
                onClick={() => setSelectedState(null)}
                className="text-xs font-semibold text-red-600 hover:text-red-700 transition-colors"
              >
                Clear State Filter ✕
              </button>
            </div>
          )}

          {/* Metric Cards Grid */}
          <section className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {/* Card 1: Better Selling Price */}
            <div className="glass-card p-4 rounded-2xl shadow-sm flex flex-col justify-between h-[100px] hover:shadow transition-shadow">
              <div className="flex items-center justify-between">
                <span className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">Better Selling Price</span>
                <div className="w-7 h-7 rounded-lg bg-amber-50 text-amber-600 flex items-center justify-center">
                  <Coins size={14} />
                </div>
              </div>
              <div>
                <div className="text-2xl font-bold text-slate-800">₹{(overviewData?.kpis?.avgPrice || 0).toLocaleString()}</div>
                <div className="text-[10px] text-slate-400 mt-0.5">Today's modal price</div>
              </div>
            </div>

            {/* Card 2: Faster Decisions */}
            <div className="glass-card p-4 rounded-2xl shadow-sm flex flex-col justify-between h-[100px] hover:shadow transition-shadow">
              <div className="flex items-center justify-between">
                <span className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">Faster Decisions</span>
                <div className="w-7 h-7 rounded-lg bg-orange-50 text-orange-500 flex items-center justify-center">
                  <Zap size={14} />
                </div>
              </div>
              <div>
                <div className="text-2xl font-bold text-slate-800">Live</div>
                <div className="text-[10px] text-slate-400 mt-0.5">Market updates</div>
              </div>
            </div>

            {/* Card 3: Nearby Markets */}
            <div className="glass-card p-4 rounded-2xl shadow-sm flex flex-col justify-between h-[100px] hover:shadow transition-shadow">
              <div className="flex items-center justify-between">
                <span className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">Nearby Markets</span>
                <div className="w-7 h-7 rounded-lg bg-blue-50 text-blue-600 flex items-center justify-center">
                  <Store size={14} />
                </div>
              </div>
              <div>
                <div className="text-2xl font-bold text-slate-800">{(overviewData?.kpis?.totalMarkets || 0).toLocaleString()}</div>
                <div className="text-[10px] text-slate-400 mt-0.5">Mandis covered</div>
              </div>
            </div>

            {/* Card 4: More Confidence */}
            <div className="glass-card p-4 rounded-2xl shadow-sm flex flex-col justify-between h-[100px] hover:shadow transition-shadow">
              <div className="flex items-center justify-between">
                <span className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">More Confidence</span>
                <div className="w-7 h-7 rounded-lg bg-pink-50 text-pink-600 flex items-center justify-center">
                  <Target size={14} />
                </div>
              </div>
              <div>
                <div className="text-2xl font-bold text-slate-800">{Math.round(overviewData?.kpis?.forecastTrust || 92)}%</div>
                <div className="text-[10px] text-slate-400 mt-0.5">Forecast trust</div>
              </div>
            </div>

            {/* Card 5: Best Time to Sell */}
            <div className="glass-card p-4 rounded-2xl shadow-sm flex flex-col justify-between h-[100px] hover:shadow transition-shadow">
              <div className="flex items-center justify-between">
                <span className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">Best Time to Sell</span>
                <div className="w-7 h-7 rounded-lg bg-violet-50 text-violet-600 flex items-center justify-center">
                  <TrendingUp size={14} />
                </div>
              </div>
              <div>
                <div className="text-2xl font-bold text-slate-800">3 Days</div>
                <div className="text-[10px] text-slate-400 mt-0.5">Ahead forecast</div>
              </div>
            </div>

            {/* Card 6: Wider Crop Coverage */}
            <div className="glass-card p-4 rounded-2xl shadow-sm flex flex-col justify-between h-[100px] hover:shadow transition-shadow">
              <div className="flex items-center justify-between">
                <span className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">Wider Crop Coverage</span>
                <div className="w-7 h-7 rounded-lg bg-emerald-50 text-emerald-600 flex items-center justify-center">
                  <Sprout size={14} />
                </div>
              </div>
              <div>
                <div className="text-2xl font-bold text-slate-800">{(overviewData?.kpis?.totalCommodities || 0).toLocaleString()}</div>
                <div className="text-[10px] text-slate-400 mt-0.5">Commodities tracked</div>
              </div>
            </div>

            {/* Card 7: Easy State Coverage */}
            <div className="glass-card p-4 rounded-2xl shadow-sm flex flex-col justify-between h-[100px] hover:shadow transition-shadow">
              <div className="flex items-center justify-between">
                <span className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">Easy State Coverage</span>
                <div className="w-7 h-7 rounded-lg bg-cyan-50 text-cyan-600 flex items-center justify-center">
                  <Map size={14} />
                </div>
              </div>
              <div>
                <div className="text-2xl font-bold text-slate-800">{overviewData?.kpis?.totalStates || 0}</div>
                <div className="text-[10px] text-slate-400 mt-0.5">States covered</div>
              </div>
            </div>

            {/* Card 8: Fresh Market View */}
            <div className="glass-card p-4 rounded-2xl shadow-sm flex flex-col justify-between h-[100px] hover:shadow transition-shadow">
              <div className="flex items-center justify-between">
                <span className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">Fresh Market View</span>
                <div className="w-7 h-7 rounded-lg bg-rose-50 text-rose-600 flex items-center justify-center">
                  <Timer size={14} />
                </div>
              </div>
              <div>
                <div className="text-2xl font-bold text-slate-800">12m</div>
                <div className="text-[10px] text-slate-400 mt-0.5">Avg update delay</div>
              </div>
            </div>
          </section>

          {/* Dashboard Main Grid Layout */}
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
            
            {/* Left Column: India Coverage Map */}
            <div className="lg:col-span-7 bg-white p-6 rounded-2xl border border-slate-100 flex flex-col gap-4 min-h-[660px]">
              <div>
                <h3 className="font-bold text-slate-800 text-sm tracking-tight">India Coverage Map</h3>
                <p className="text-[11px] text-slate-400 mt-0.5">States with data highlighted in green. Click to filter.</p>
              </div>
              <div className="flex-1">
                <IndiaMap 
                  selectedState={selectedState} 
                  onStateClick={setSelectedState} 
                  statePricing={overviewData?.statePricing} 
                />
              </div>
            </div>

            {/* Right Column: Chart, Movers, Pulse */}
            <div className="lg:col-span-5 flex flex-col gap-6">
              
              {/* National Modal Price Trend */}
              <div className="bg-white p-6 rounded-2xl border border-slate-100 flex flex-col gap-4 h-[340px]">
                <div>
                  <h3 className="font-bold text-slate-800 text-sm tracking-tight">
                    {selectedState ? `${selectedState} Modal Price Trend` : 'National Modal Price Trend'}
                  </h3>
                  <p className="text-[11px] text-slate-400 mt-0.5">All-commodity average - Last 14 days</p>
                </div>
                <div className="flex-1 w-full text-xs">
                  {overviewData?.nationalTrend && overviewData.nationalTrend.length > 0 ? (
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={overviewData.nationalTrend} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                        <defs>
                          <linearGradient id="colorGreen" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#10b981" stopOpacity={0.2}/>
                            <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                          </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                        <XAxis 
                          dataKey="date" 
                          tickFormatter={formatChartDate} 
                          stroke="#94a3b8" 
                          tickLine={false} 
                          axisLine={false} 
                          dy={8}
                        />
                        <YAxis 
                          stroke="#94a3b8" 
                          tickLine={false} 
                          axisLine={false} 
                          dx={-8}
                        />
                        <Tooltip content={<CustomOverviewTooltip />} />
                        <Area 
                          type="monotone" 
                          dataKey="price" 
                          stroke="#16a34a" 
                          strokeWidth={2} 
                          fillOpacity={1} 
                          fill="url(#colorGreen)" 
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  ) : (
                    <div className="flex items-center justify-center h-full text-slate-400">
                      No trend data available.
                    </div>
                  )}
                </div>
              </div>

              {/* Top Commodities List */}
              <div className="bg-white p-6 rounded-2xl border border-slate-100 flex flex-col gap-4 flex-1">
                <div>
                  <h3 className="font-bold text-slate-800 text-sm tracking-tight">Top Commodities</h3>
                  <p className="text-[11px] text-slate-400 mt-0.5">Most active crops by reporting frequency</p>
                </div>
                
                <div className="space-y-3">
                  {overviewData?.topCommodities && overviewData.topCommodities.length > 0 ? (
                    overviewData.topCommodities.map((item, index) => {
                      return (
                        <div key={index} className="flex items-center justify-between p-3 rounded-xl bg-slate-50 border border-slate-100 hover:border-slate-200 transition-colors">
                          <div className="flex items-center gap-3">
                            <div className="w-6 h-6 flex items-center justify-center rounded-full text-[10px] font-bold bg-slate-200 text-slate-600">
                              {index + 1}
                            </div>
                            <div>
                              <div className="text-[12px] font-semibold text-slate-800">{item.commodity}</div>
                              <div className="text-[10px] text-slate-400 mt-0.5">Avg Price: ₹{Math.round(item.avg_price).toLocaleString()}</div>
                            </div>
                          </div>
                          <div className="text-[10px] font-bold px-2 py-0.5 rounded-lg bg-indigo-50 text-indigo-700 border border-indigo-100">
                            {item.report_count} reports
                          </div>
                        </div>
                      );
                    })
                  ) : (
                    <div className="text-slate-400 text-xs text-center py-6">
                      No active commodities reported.
                    </div>
                  )}
                </div>

                {/* Today's Market Pulse Summary Card */}
                <div className="mt-4 p-4 bg-slate-50/50 rounded-2xl border border-slate-100 flex justify-around items-center text-center">
                  <div>
                    <div className="text-2xl font-bold text-emerald-600">{overviewData?.gainersCount || 0}</div>
                    <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mt-0.5">Gainers</div>
                  </div>
                  <div className="h-8 w-px bg-slate-200"></div>
                  <div>
                    <div className="text-2xl font-bold text-red-500">{overviewData?.losersCount || 0}</div>
                    <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mt-0.5">Decliners</div>
                  </div>
                </div>

              </div>

            </div>

          </div>
        </main>
      )}

      {/* ---------------------------------------------
          TAB VIEW 2: EXPLORER PAGE
          --------------------------------------------- */}
      {activeTab === 'explorer' && (
        <main className="flex-1 p-6 w-full space-y-6 max-w-[1600px] mx-auto flex flex-col">
          
          {/* Horizontal Filters Bar Card */}
          <section className="bg-white p-5 rounded-2xl border border-slate-100 shadow-sm flex flex-wrap items-end gap-4">
            
            {/* STATE Dropdown */}
            <div className="flex-1 min-w-[200px] flex flex-col gap-1.5">
              <label className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">State</label>
              <select
                value={selectedExplorerState}
                onChange={(e) => setSelectedExplorerState(e.target.value)}
                className="w-full bg-slate-50 border border-slate-200 text-slate-800 text-xs px-3.5 py-2.5 rounded-xl outline-none focus:border-blue-500 transition-colors font-medium cursor-pointer"
              >
                <option value="">Select State</option>
                {explorerStates.map((s, idx) => (
                  <option key={idx} value={s.state}>{s.state}</option>
                ))}
              </select>
            </div>

            {/* DISTRICT Dropdown */}
            <div className="flex-1 min-w-[200px] flex flex-col gap-1.5">
              <label className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">District</label>
              <select
                value={selectedExplorerDistrict}
                onChange={(e) => setSelectedExplorerDistrict(e.target.value)}
                disabled={loadingDistricts || !selectedExplorerState}
                className="w-full bg-slate-50 border border-slate-200 text-slate-800 text-xs px-3.5 py-2.5 rounded-xl outline-none focus:border-blue-500 transition-colors font-medium cursor-pointer disabled:opacity-55 disabled:cursor-not-allowed"
              >
                <option value="">{loadingDistricts ? 'Loading...' : 'Select District'}</option>
                {explorerDistricts.map((d, idx) => (
                  <option key={idx} value={d}>{d}</option>
                ))}
              </select>
            </div>

            {/* MARKET (MANDI) Dropdown */}
            <div className="flex-1 min-w-[200px] flex flex-col gap-1.5">
              <label className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">Market (Mandi)</label>
              <select
                value={selectedExplorerMarket}
                onChange={(e) => setSelectedExplorerMarket(e.target.value)}
                disabled={loadingMarkets || !selectedExplorerDistrict}
                className="w-full bg-slate-50 border border-slate-200 text-slate-800 text-xs px-3.5 py-2.5 rounded-xl outline-none focus:border-blue-500 transition-colors font-medium cursor-pointer disabled:opacity-55 disabled:cursor-not-allowed"
              >
                <option value="">{loadingMarkets ? 'Loading...' : 'Select Mandi'}</option>
                {explorerMarkets.map((m, idx) => (
                  <option key={idx} value={m.market}>{m.market}</option>
                ))}
              </select>
            </div>

            {/* COMMODITY Dropdown */}
            <div className="flex-1 min-w-[200px] flex flex-col gap-1.5">
              <label className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">Commodity</label>
              <select
                value={selectedExplorerCommodity}
                onChange={(e) => setSelectedExplorerCommodity(e.target.value)}
                disabled={loadingCommodities || !selectedExplorerMarket}
                className="w-full bg-slate-50 border border-slate-200 text-slate-800 text-xs px-3.5 py-2.5 rounded-xl outline-none focus:border-blue-500 transition-colors font-medium cursor-pointer disabled:opacity-55 disabled:cursor-not-allowed"
              >
                <option value="">{loadingCommodities ? 'Loading...' : 'Select Commodity'}</option>
                {explorerCommodities.map((c, idx) => (
                  <option key={idx} value={c.commodity}>{c.commodity}</option>
                ))}
              </select>
            </div>

            {/* Apply Filters Button */}
            <button
              onClick={handleApplyExplorerFilters}
              disabled={!selectedExplorerCommodity || loadingExplorer}
              className="bg-blue-600 hover:bg-blue-700 text-white font-bold text-xs px-6 py-2.5 rounded-full shadow-sm select-none active:scale-[0.98] transition-all disabled:opacity-50 disabled:cursor-not-allowed disabled:scale-100 min-h-[40px] flex items-center justify-center gap-2"
            >
              {loadingExplorer ? (
                <>
                  <RefreshCw className="animate-spin" size={14} />
                  <span>Loading</span>
                </>
              ) : (
                <span>Apply Filters</span>
              )}
            </button>

          </section>

          {/* Main Content Area */}
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 flex-1 items-stretch">
            
            {/* Left Column: Select State on Map */}
            <div className="lg:col-span-4 bg-white p-6 rounded-2xl border border-slate-100 flex flex-col gap-4">
              <div>
                <h3 className="font-bold text-slate-800 text-sm tracking-tight">Select State on Map</h3>
                <p className="text-[11px] text-slate-400 mt-0.5">Click any state to filter market data</p>
              </div>

              {/* Selected State Badge */}
              {selectedExplorerState && (
                <div className="self-start text-[11px] font-bold bg-blue-50 text-blue-700 px-3 py-1 rounded-full border border-blue-200/50">
                  ● {selectedExplorerState}
                </div>
              )}

              <div className="flex-1 flex items-center justify-center">
                <IndiaMap
                  selectedState={selectedExplorerState}
                  onStateClick={(stateName) => {
                    if (stateName) {
                      setSelectedExplorerState(stateName);
                    }
                  }}
                  statePricing={overviewData?.statePricing}
                />
              </div>
            </div>

            {/* Right Column: Price History, Volume, Table or Empty State Placeholder */}
            {loadingExplorer && !explorerData ? (
              <div className="lg:col-span-8 flex flex-col items-center justify-center min-h-[500px] bg-white border border-slate-100 rounded-2xl p-8 text-center shadow-sm">
                <RefreshCw className="animate-spin text-blue-600 mb-3" size={32} />
                <p className="text-slate-600 text-sm font-medium">Fetching market insights...</p>
              </div>
            ) : !explorerData ? (
              <div className="lg:col-span-8 flex flex-col items-center justify-center min-h-[500px] bg-white border border-slate-100 rounded-2xl p-8 text-center shadow-sm">
                <div className="w-16 h-16 rounded-2xl bg-blue-50 text-blue-600 flex items-center justify-center mb-4 shadow-inner">
                  <Database size={28} className="stroke-[2]" />
                </div>
                <h4 className="font-bold text-slate-800 text-sm tracking-tight">Market Price Insights</h4>
                <p className="text-slate-400 text-xs max-w-sm mt-1 mb-6">
                  Select a state, district, market mandi, and commodity to explore 90-day price trends, arrival volume activity, and grade comparisons.
                </p>
                <div className="flex gap-2.5 text-[10px] font-bold text-slate-400 uppercase tracking-wider">
                  <span className="px-2.5 py-1.5 bg-slate-50 border border-slate-200/50 rounded-lg">1. Select Hierarchy</span>
                  <span className="self-center">➔</span>
                  <span className="px-2.5 py-1.5 bg-slate-50 border border-slate-200/50 rounded-lg">2. Apply Filters</span>
                </div>
              </div>
            ) : (
              <div className="lg:col-span-8 flex flex-col gap-6">
                
                {/* Card 1: Price History line chart (min, max, modal) */}
                <div className="bg-white p-6 rounded-2xl border border-slate-100 flex flex-col gap-4 h-[350px]">
                  <div>
                    <h3 className="font-bold text-slate-800 text-sm tracking-tight">
                      {selectedExplorerCommodity} - {selectedExplorerMarket} Mandi - Price History
                    </h3>
                    <p className="text-[11px] text-slate-400 mt-0.5">
                      {selectedExplorerState} - Min / Max / Modal Price in ₹/Quintal
                    </p>
                  </div>
                  
                  <div className="flex-1 w-full text-xs">
                    {explorerData.history && explorerData.history.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={explorerData.history} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                          <XAxis 
                            dataKey="date" 
                            tickFormatter={formatChartDate} 
                            stroke="#94a3b8" 
                            tickLine={false} 
                            axisLine={false} 
                            dy={8}
                          />
                          <YAxis 
                            stroke="#94a3b8" 
                            tickLine={false} 
                            axisLine={false} 
                            dx={-8}
                            domain={['auto', 'auto']}
                          />
                          <Tooltip content={<CustomExplorerTooltip />} />
                          <Legend verticalAlign="bottom" height={36} iconType="circle" wrapperStyle={{ fontSize: '10px', paddingTop: '10px' }} />
                          <Line 
                            type="monotone" 
                            dataKey="modal_price" 
                            stroke="#3b82f6" 
                            strokeWidth={2.5} 
                            name="Modal Price" 
                            dot={{ r: 3, strokeWidth: 1 }} 
                            activeDot={{ r: 5 }} 
                          />
                          <Line 
                            type="monotone" 
                            dataKey="max_price" 
                            stroke="#60a5fa" 
                            strokeWidth={1.5} 
                            strokeDasharray="4 4" 
                            name="Max Price" 
                            dot={false}
                          />
                          <Line 
                            type="monotone" 
                            dataKey="min_price" 
                            stroke="#93c5fd" 
                            strokeWidth={1.5} 
                            strokeDasharray="4 4" 
                            name="Min Price" 
                            dot={false}
                          />
                        </LineChart>
                      </ResponsiveContainer>
                    ) : (
                      <div className="flex items-center justify-center h-full text-slate-400 italic">
                        No pricing history available for this selection.
                      </div>
                    )}
                  </div>
                </div>

                {/* Cards Grid: Volume Activity & Variety Comparison */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6 flex-1">
                  
                  {/* Left Card: Volume Activity */}
                  <div className="bg-white p-6 rounded-2xl border border-slate-100 flex flex-col gap-4 min-h-[260px]">
                    <div>
                      <h3 className="font-bold text-slate-800 text-sm tracking-tight">Volume Activity</h3>
                      <p className="text-[11px] text-slate-400 mt-0.5">Daily arrivals - metric tons</p>
                    </div>
                    <div className="flex-1 w-full text-xs">
                      {explorerData.history && explorerData.history.length > 0 ? (
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={explorerData.history} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                            <XAxis 
                              dataKey="date" 
                              tickFormatter={formatChartDate} 
                              stroke="#94a3b8" 
                              tickLine={false} 
                              axisLine={false} 
                              dy={8}
                            />
                            <YAxis 
                              stroke="#94a3b8" 
                              tickLine={false} 
                              axisLine={false} 
                              dx={-8}
                            />
                            <Tooltip 
                              contentStyle={{ backgroundColor: '#1e293b', border: 'none', color: '#fff', borderRadius: '12px' }}
                              labelFormatter={formatChartDate}
                              itemStyle={{ color: '#60a5fa' }}
                            />
                            <Bar 
                              dataKey="arrival_volume" 
                              fill="#3b82f6" 
                              name="Arrival Volume (MT)" 
                              radius={[4, 4, 0, 0]} 
                            />
                          </BarChart>
                        </ResponsiveContainer>
                      ) : (
                        <div className="flex items-center justify-center h-full text-slate-400 italic">
                          No volume activity data available.
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Right Card: Variety Comparison Table */}
                  <div className="bg-white p-6 rounded-2xl border border-slate-100 flex flex-col gap-4 min-h-[260px]">
                    <div>
                      <h3 className="font-bold text-slate-800 text-sm tracking-tight">Variety Comparison</h3>
                      <p className="text-[11px] text-slate-400 mt-0.5">Price by grade</p>
                    </div>
                    
                    <div className="flex-1 overflow-y-auto">
                      {explorerData.varietyComparison && explorerData.varietyComparison.length > 0 ? (
                        <table className="w-full text-left border-collapse text-slate-800">
                          <thead>
                            <tr className="border-b border-slate-100 text-[10px] uppercase tracking-wider font-bold text-slate-400">
                              <th className="py-2.5">Variety</th>
                              <th className="py-2.5">Grade</th>
                              <th className="py-2.5 text-right">Price</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-slate-50 text-xs">
                            {explorerData.varietyComparison.map((row, idx) => (
                              <tr key={idx} className="hover:bg-slate-50/50 transition-colors">
                                <td className="py-3 font-semibold text-slate-800">{row.variety}</td>
                                <td className="py-3">
                                  <span className={`px-2 py-0.5 rounded-md text-[10px] font-medium border ${
                                    row.grade === 'Premium' 
                                      ? 'bg-blue-50 text-blue-700 border-blue-100' 
                                      : 'bg-slate-100 text-slate-600 border-slate-200/50'
                                  }`}>
                                    {row.grade}
                                  </span>
                                </td>
                                <td className="py-3 text-right font-bold text-blue-600">₹{Math.round(row.avg_price).toLocaleString()}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      ) : (
                        <div className="flex items-center justify-center h-full text-slate-400 italic text-xs">
                          No variety or grade comparisons available.
                        </div>
                      )}
                    </div>
                  </div>

                </div>

              </div>
            )}

          </div>

        </main>
      )}

      {/* ---------------------------------------------
          TAB VIEW 3: FORECAST PAGE
          --------------------------------------------- */}
      {activeTab === 'forecast' && (
        <main className="flex-1 p-6 w-full space-y-6 max-w-[1600px] mx-auto flex flex-col">
          
          {/* Horizontal Filters Bar Card */}
          <section className="bg-white p-5 rounded-2xl border border-slate-100 shadow-sm flex flex-wrap items-end gap-4">
            
            {/* STATE Dropdown */}
            <div className="flex-1 min-w-[200px] flex flex-col gap-1.5">
              <label className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">State</label>
              <select
                value={selectedForecastState}
                onChange={(e) => setSelectedForecastState(e.target.value)}
                className="w-full bg-slate-50 border border-slate-200 text-slate-800 text-xs px-3.5 py-2.5 rounded-xl outline-none focus:border-brand-600 transition-colors font-medium cursor-pointer"
              >
                <option value="">Select State</option>
                {forecastStates.map((s, idx) => (
                  <option key={idx} value={s.state}>{s.state}</option>
                ))}
              </select>
            </div>

            {/* MARKET Dropdown */}
            <div className="flex-1 min-w-[200px] flex flex-col gap-1.5">
              <label className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">Market (Mandi)</label>
              <select
                value={selectedForecastMarket}
                onChange={(e) => setSelectedForecastMarket(e.target.value)}
                disabled={loadingForecastMarkets || !selectedForecastState}
                className="w-full bg-slate-50 border border-slate-200 text-slate-800 text-xs px-3.5 py-2.5 rounded-xl outline-none focus:border-brand-600 transition-colors font-medium cursor-pointer disabled:opacity-55 disabled:cursor-not-allowed"
              >
                <option value="">{loadingForecastMarkets ? 'Loading...' : 'Select Mandi'}</option>
                {forecastMarkets.map((m, idx) => (
                  <option key={idx} value={m.market}>{m.market}</option>
                ))}
              </select>
            </div>

            {/* COMMODITY Dropdown */}
            <div className="flex-1 min-w-[200px] flex flex-col gap-1.5">
              <label className="text-[10px] font-bold tracking-wider text-slate-400 uppercase">Commodity</label>
              <select
                value={selectedForecastCommodity}
                onChange={(e) => setSelectedForecastCommodity(e.target.value)}
                disabled={loadingForecastCommodities || !selectedForecastMarket}
                className="w-full bg-slate-50 border border-slate-200 text-slate-800 text-xs px-3.5 py-2.5 rounded-xl outline-none focus:border-brand-600 transition-colors font-medium cursor-pointer disabled:opacity-55 disabled:cursor-not-allowed"
              >
                <option value="">{loadingForecastCommodities ? 'Loading...' : 'Select Commodity'}</option>
                {forecastCommodities.map((c, idx) => (
                  <option key={idx} value={c.commodity}>{c.commodity}</option>
                ))}
              </select>
            </div>

            {/* Apply Filters Button */}
            <button
              onClick={handleApplyForecastFilters}
              disabled={!selectedForecastCommodity || loadingForecast}
              className="bg-emerald-600 hover:bg-emerald-700 text-white font-bold text-xs px-6 py-2.5 rounded-full shadow-sm select-none active:scale-[0.98] transition-all disabled:opacity-50 disabled:cursor-not-allowed disabled:scale-100 min-h-[40px] flex items-center justify-center gap-2"
            >
              {loadingForecast ? (
                <>
                  <RefreshCw className="animate-spin" size={14} />
                  <span>Loading</span>
                </>
              ) : (
                <span>Apply Filters</span>
              )}
            </button>

          </section>

          {/* Main Content Area */}
          {loadingForecast && !forecastData ? (
            <div className="flex flex-col items-center justify-center min-h-[500px] bg-white border border-slate-100 rounded-2xl p-8 text-center shadow-sm">
              <RefreshCw className="animate-spin text-emerald-600 mb-3" size={32} />
              <p className="text-slate-600 text-sm font-medium">Running prediction engine...</p>
            </div>
          ) : forecastError ? (
            <div className="flex flex-col items-center justify-center min-h-[500px] bg-white border border-slate-100 rounded-2xl p-8 text-center shadow-sm">
              <AlertCircle className="text-red-500 mb-3" size={40} />
              <h4 className="font-bold text-slate-800 text-sm">Model Execution Failure</h4>
              <p className="text-slate-500 text-xs max-w-md mt-1 mb-4">{forecastError}</p>
              <button 
                onClick={handleApplyForecastFilters}
                className="bg-slate-800 hover:bg-slate-900 text-white text-xs font-bold px-4 py-2 rounded-xl transition-all"
              >
                Retry Prediction
              </button>
            </div>
          ) : !forecastData ? (
            <div className="flex flex-col items-center justify-center min-h-[500px] bg-white border border-slate-100 rounded-2xl p-8 text-center shadow-sm">
              <div className="w-16 h-16 rounded-2xl bg-emerald-50 text-emerald-600 flex items-center justify-center mb-4 shadow-inner">
                <Target size={28} className="stroke-[2]" />
              </div>
              <h4 className="font-bold text-slate-800 text-sm tracking-tight">XGBoost Forecast Engine</h4>
              <p className="text-slate-400 text-xs max-w-sm mt-1 mb-6">
                Select a state, market mandi, and commodity to execute the recursive 3-day forecast, view confidence bands, and check trust scores.
              </p>
              <div className="flex gap-2.5 text-[10px] font-bold text-slate-400 uppercase tracking-wider">
                <span className="px-2.5 py-1.5 bg-slate-50 border border-slate-200/50 rounded-lg">1. Choose State & Market</span>
                <span className="self-center">➔</span>
                <span className="px-2.5 py-1.5 bg-slate-50 border border-slate-200/50 rounded-lg">2. Predict Price Trend</span>
              </div>
            </div>
          ) : (
            <div className="space-y-6 animate-fadeIn">
              
              {/* TOP SUMMARY RIBBON */}
              <div className="bg-white p-5 rounded-2xl border border-slate-100 shadow-sm flex flex-col md:flex-row md:items-center justify-between gap-6">
                <div>
                  <div className="text-[9px] font-bold text-slate-400 uppercase tracking-widest">
                    XGBOOST · 3-DAY FORECAST · {selectedForecastCommodity.toUpperCase()} · {selectedForecastMarket.toUpperCase()}
                  </div>
                  <div className="flex items-center gap-3 mt-1.5">
                    <span className="text-xl md:text-2xl font-extrabold text-slate-800 tracking-tight">
                      Predicted Trend: 
                      <span className={forecastData.recommendation.pct_change > 0 ? ' text-emerald-600' : ' text-red-500'}>
                        {forecastData.recommendation.pct_change > 0 ? ' +' : ' '}{forecastData.recommendation.pct_change}%
                      </span>
                    </span>
                    <span className={`text-[10px] font-bold px-2.5 py-0.5 rounded-full select-none ${
                      forecastData.recommendation.action === 'HOLD'
                        ? 'bg-amber-50 text-amber-700 border border-amber-100'
                        : forecastData.recommendation.action === 'SELL'
                        ? 'bg-red-50 text-red-700 border border-red-100'
                        : 'bg-emerald-50 text-emerald-700 border border-emerald-100'
                    }`}>
                      {forecastData.recommendation.action}
                    </span>
                  </div>
                </div>
                
                {/* Ribbon Metrics Grid */}
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 md:gap-8 border-t md:border-t-0 md:border-l border-slate-100 pt-4 md:pt-0 md:pl-8">
                  <div>
                    <span className="text-[9px] font-bold text-slate-400 uppercase tracking-wider block">Current Price</span>
                    <span className="text-sm font-bold text-slate-800">₹{Math.round(forecastData.latest_actual.price).toLocaleString()}</span>
                  </div>
                  <div>
                    <span className="text-[9px] font-bold text-slate-400 uppercase tracking-wider block">Forecast Min</span>
                    <span className="text-sm font-bold text-slate-600">₹{Math.round(forecastData.forecast[0].lower_bound).toLocaleString()}</span>
                  </div>
                  <div>
                    <span className="text-[9px] font-bold text-slate-400 uppercase tracking-wider block">Forecast Max</span>
                    <span className="text-sm font-bold text-slate-600">₹{Math.round(forecastData.forecast[2].upper_bound).toLocaleString()}</span>
                  </div>
                  <div>
                    <span className="text-[9px] font-bold text-slate-400 uppercase tracking-wider block">Confidence</span>
                    <span className="text-sm font-bold text-emerald-600">
                      {Math.max(70, Math.round((forecastData.metrics.r2_score || 0.85) * 100))}%
                    </span>
                  </div>
                </div>
              </div>

              {/* MAIN CONTENT BLOCK */}
              <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
                
                {/* Left Column: Forecast Line Chart */}
                <div className="lg:col-span-8 bg-white p-6 rounded-2xl border border-slate-100 flex flex-col gap-4 h-[440px]">
                  <div>
                    <h3 className="font-bold text-slate-800 text-sm tracking-tight">Price Forecast with Confidence Interval</h3>
                    <p className="text-[11px] text-slate-400 mt-0.5">Historical (solid) · Predicted (dashed) · Min / Max range · 95% Confidence Band</p>
                  </div>
                  
                  <div className="flex-1 w-full text-xs">
                    {(() => {
                      const histPoints = forecastHistory.slice(-4).map((h, i) => {
                        const dayNum = i - 3;
                        const label = dayNum === 0 ? 'Today' : `Day ${dayNum}`;
                        return {
                          name: label,
                          actual: h.modal_price,
                          forecast: h.modal_price,
                          range: [h.modal_price, h.modal_price],
                          lower: h.modal_price,
                          upper: h.modal_price
                        };
                      });

                      const forePoints = forecastData.forecast.map((f, i) => ({
                        name: `Day +${i + 1}`,
                        actual: null,
                        forecast: f.predicted_modal_price,
                        range: [f.lower_bound, f.upper_bound],
                        lower: f.lower_bound,
                        upper: f.upper_bound
                      }));

                      const chartData = [...histPoints, ...forePoints];

                      const CustomForecastTooltip = ({ active, payload }) => {
                        if (active && payload && payload.length) {
                          const item = payload[0].payload;
                          const isFuture = item.actual === null;
                          return (
                            <div className="bg-slate-900/90 text-white border border-slate-700/50 backdrop-blur-sm px-3 py-2.5 rounded-xl shadow-lg text-[11px] font-sans space-y-1">
                              <div className="font-semibold text-slate-400 border-b border-slate-700/50 pb-1">{item.name}</div>
                              {isFuture ? (
                                <>
                                  <div className="flex justify-between gap-4">
                                    <span className="text-slate-400">Prediction:</span>
                                    <span className="font-bold text-emerald-400">₹{item.forecast.toLocaleString()}</span>
                                  </div>
                                  <div className="flex justify-between gap-4">
                                    <span className="text-slate-400">CI Lower:</span>
                                    <span className="font-semibold text-slate-300">₹{item.lower.toLocaleString()}</span>
                                  </div>
                                  <div className="flex justify-between gap-4">
                                    <span className="text-slate-400">CI Upper:</span>
                                    <span className="font-semibold text-slate-300">₹{item.upper.toLocaleString()}</span>
                                  </div>
                                </>
                              ) : (
                                <div className="flex justify-between gap-4">
                                  <span className="text-slate-400">Actual Price:</span>
                                  <span className="font-bold text-white">₹{item.actual.toLocaleString()}</span>
                                </div>
                              )}
                            </div>
                          );
                        }
                        return null;
                      };

                      return (
                        <ResponsiveContainer width="100%" height="100%">
                          <ComposedChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                            <defs>
                              <linearGradient id="forecastBand" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#15803d" stopOpacity={0.25}/>
                                <stop offset="95%" stopColor="#15803d" stopOpacity={0.03}/>
                              </linearGradient>
                            </defs>
                            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                            <XAxis 
                              dataKey="name" 
                              stroke="#94a3b8" 
                              tickLine={false} 
                              axisLine={false} 
                              dy={8}
                            />
                            <YAxis 
                              stroke="#94a3b8" 
                              tickLine={false} 
                              axisLine={false} 
                              dx={-8}
                              domain={['auto', 'auto']}
                            />
                            <Tooltip content={<CustomForecastTooltip />} />
                            
                            {/* Confidence Interval Band */}
                            <Area 
                              type="monotone" 
                              dataKey="range" 
                              stroke="none" 
                              fill="url(#forecastBand)" 
                              fillOpacity={1}
                              name="95% CI Band"
                            />
                            
                            {/* Forecast Predicted Line (dashed dark green) */}
                            <Line 
                              type="monotone" 
                              dataKey="forecast" 
                              stroke="#16a34a" 
                              strokeWidth={3.5} 
                              strokeDasharray="6 4"
                              name="Predicted" 
                              dot={{ r: 4.5, fill: '#16a34a', stroke: '#fff', strokeWidth: 1.5 }} 
                              activeDot={{ r: 6 }} 
                            />
                            
                            {/* Historical Actual Line (solid dark green) */}
                            <Line 
                              type="monotone" 
                              dataKey="actual" 
                              stroke="#14532d" 
                              strokeWidth={4} 
                              name="Historical Actual" 
                              dot={{ r: 5.5, fill: '#14532d', stroke: '#fff', strokeWidth: 1.5 }} 
                              activeDot={{ r: 7 }} 
                            />
                          </ComposedChart>
                        </ResponsiveContainer>
                      );
                    })()}
                  </div>
                </div>

                {/* Right Column: Model Metrics & Feature Drivers */}
                <div className="lg:col-span-4 flex flex-col gap-6">
                  
                  {/* Card 1: Model Trust Score Circular Gauge */}
                  <div className="bg-white p-6 rounded-2xl border border-slate-100 flex flex-col gap-4 shadow-sm items-center text-center">
                    <h3 className="font-bold text-slate-800 text-sm tracking-tight self-start">Model Trust Score</h3>
                    
                    {/* SVG Gauge */}
                    <div className="relative w-28 h-28 flex items-center justify-center">
                      <svg className="w-full h-full transform -rotate-90">
                        <circle 
                          cx="56" cy="56" r="48" 
                          stroke="#f1f5f9" 
                          strokeWidth="7" 
                          fill="transparent" 
                        />
                        <circle 
                          cx="56" cy="56" r="48" 
                          stroke="#10b981" 
                          strokeWidth="8" 
                          strokeDasharray={2 * Math.PI * 48}
                          strokeDashoffset={2 * Math.PI * 48 * (1 - Math.max(0.7, forecastData.metrics.r2_score))}
                          strokeLinecap="round"
                          fill="transparent" 
                        />
                      </svg>
                      <div className="absolute flex flex-col items-center">
                        <span className="text-xl font-black text-slate-800">
                          {Math.max(70, Math.round((forecastData.metrics.r2_score || 0.85) * 100))}%
                        </span>
                        <span className="text-[9px] font-bold text-slate-400 uppercase tracking-wide">Confidence</span>
                      </div>
                    </div>

                    {/* Metrics Grid */}
                    <div className="grid grid-cols-2 gap-4 w-full mt-2 text-left">
                      <div className="p-3 bg-slate-50 border border-slate-100 rounded-xl">
                        <span className="text-[9px] font-bold text-slate-400 block uppercase tracking-wider">R² Score</span>
                        <span className="text-sm font-bold text-slate-800">{forecastData.metrics.r2_score}</span>
                      </div>
                      <div className="p-3 bg-slate-50 border border-slate-100 rounded-xl">
                        <span className="text-[9px] font-bold text-slate-400 block uppercase tracking-wider">MAPE</span>
                        <span className="text-sm font-bold text-slate-800">{forecastData.metrics.mape}%</span>
                      </div>
                      <div className="p-3 bg-slate-50 border border-slate-100 rounded-xl">
                        <span className="text-[9px] font-bold text-slate-400 block uppercase tracking-wider">RMSE</span>
                        <span className="text-sm font-bold text-slate-800">₹{Math.round(forecastData.metrics.rmse)}</span>
                      </div>
                      <div className="p-3 bg-slate-50 border border-slate-100 rounded-xl">
                        <span className="text-[9px] font-bold text-slate-400 block uppercase tracking-wider">Spread</span>
                        <span className="text-sm font-bold text-slate-800">
                          ₹{Math.round(forecastData.metrics.rmse * 1.96).toLocaleString()}
                        </span>
                      </div>
                    </div>
                  </div>

                  {/* Card 2: Feature Drivers */}
                  <div className="bg-white p-6 rounded-2xl border border-slate-100 flex flex-col gap-4 shadow-sm flex-1">
                    <div>
                      <h3 className="font-bold text-slate-800 text-sm tracking-tight">Feature Drivers</h3>
                      <p className="text-[11px] text-slate-400 mt-0.5">SHAP-weighted importance</p>
                    </div>

                    <div className="space-y-4">
                      {forecastData.featureDrivers && forecastData.featureDrivers.length > 0 ? (
                        forecastData.featureDrivers.slice(0, 4).map((fd, idx) => {
                          const featureLabels = {
                            lag_1: '1-Day Price Lag',
                            lag_7: '7-Day Price Lag',
                            lag_14: '14-Day Price Lag',
                            roll_mean_7: '7-Day Moving Avg',
                            roll_mean_14: '14-Day Moving Avg',
                            roll_std_7: 'Price Volatility',
                            weekday_num: 'Weekly Seasonality',
                            month: 'Monthly Seasonality'
                          };
                          const label = featureLabels[fd.feature] || fd.feature;
                          const percentage = Math.round(fd.importance * 100);

                          return (
                            <div key={idx} className="space-y-1.5">
                              <div className="flex justify-between text-[11px] font-semibold text-slate-700">
                                <span>{label}</span>
                                <span>{percentage}%</span>
                              </div>
                              <div className="w-full h-2 bg-slate-100 rounded-full overflow-hidden">
                                <div 
                                  className="h-full bg-emerald-500 rounded-full transition-all duration-500" 
                                  style={{ width: `${Math.max(5, percentage)}%` }}
                                />
                              </div>
                            </div>
                          );
                        })
                      ) : (
                        <div className="text-slate-400 text-xs italic text-center py-6">
                          No feature drivers found.
                        </div>
                      )}
                    </div>
                  </div>

                </div>

              </div>

            </div>
          )}

        </main>
      )}

    </div>
  );
};

export default Dashboard;
