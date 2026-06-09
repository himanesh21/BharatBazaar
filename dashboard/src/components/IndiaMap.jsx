import React, { useMemo, useState } from 'react';
import { feature } from 'topojson-client';
import { geoMercator, geoPath } from 'd3-geo';
import indiaTopo from '../data/INDIA.json';

const IndiaMap = ({ selectedState, onStateClick, statePricing }) => {
  const [hoveredState, setHoveredState] = useState(null);
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });

  const mapData = useMemo(() => {
    const key = Object.keys(indiaTopo.objects)[0];
    return feature(indiaTopo, indiaTopo.objects[key]).features;
  }, []);

  // Standard projection for India centered on the geographical middle
  const projection = geoMercator()
    .center([78.5, 22.2])
    .scale(1050)
    .translate([280, 310]);

  const pathGenerator = useMemo(() => geoPath().projection(projection), [projection]);

  return (
    <div className="relative w-full h-[640px] flex items-center justify-center bg-white rounded-2xl border border-slate-100 overflow-hidden">
      <svg viewBox="0 0 560 620" className="w-full h-full select-none max-h-[600px]">
        {mapData.map((feature, i) => {
          const stateName = feature.properties.ST_NM || feature.properties.NAME_1 || feature.properties.name || "Unknown";
          
          // Match by geometry index i (which is state_id) or by name
          const pricing = statePricing ? (statePricing[i] || statePricing[String(i)]) : null;
          const hasData = pricing && pricing.record_count > 0;
          
          const isSelected = selectedState === stateName;

          // Color scheme matching screenshots:
          // Active states are filled with a sage green color (#76b07a)
          // Inactive/no-data states are filled with a light gray (#e2e8f0)
          let fill = '#e2e8f0'; // default gray
          if (isSelected) {
            fill = '#15803d'; // dark green selected state
          } else if (hasData) {
            fill = '#76b07a'; // sage green active state
          }

          return (
            <path
              key={i}
              d={pathGenerator(feature)}
              fill={fill}
              stroke="#ffffff"
              strokeWidth={1.2}
              className="transition-all duration-200 cursor-pointer hover:opacity-85 hover:stroke-[1.8px] hover:stroke-slate-100"
              onClick={() => onStateClick(isSelected ? null : stateName)}
              onMouseEnter={(e) => {
                setHoveredState({
                  name: stateName,
                  avgPrice: pricing ? pricing.avg_price : null,
                  recordCount: pricing ? pricing.record_count : 0
                });
              }}
              onMouseMove={(e) => {
                const rect = e.currentTarget.ownerSVGElement.getBoundingClientRect();
                setTooltipPos({
                  x: e.clientX - rect.left + 15,
                  y: e.clientY - rect.top + 15
                });
              }}
              onMouseLeave={() => {
                setHoveredState(null);
              }}
            />
          );
        })}
      </svg>
      
      {/* Floating Mouse Tooltip */}
      {hoveredState && (
        <div 
          className="absolute pointer-events-none bg-slate-900/90 text-white text-xs px-3 py-2 rounded-xl shadow-xl z-50 border border-slate-700/50 backdrop-blur-sm transition-all duration-75"
          style={{ 
            left: `${tooltipPos.x}px`, 
            top: `${tooltipPos.y}px`,
            transform: 'translate(0, 0)'
          }}
        >
          <div className="font-semibold text-emerald-400">{hoveredState.name}</div>
          {hoveredState.avgPrice ? (
            <div className="mt-1 space-y-0.5 text-[11px] text-slate-300">
              <div>Avg Price: <span className="font-semibold text-white">₹{hoveredState.avgPrice}</span></div>
              <div>Records: <span className="font-semibold text-white">{hoveredState.recordCount.toLocaleString()}</span></div>
            </div>
          ) : (
            <div className="text-[10px] text-slate-400 mt-0.5">No price data available</div>
          )}
        </div>
      )}
      
      {/* Legend inside map container */}
      <div className="absolute bottom-4 left-4 flex flex-col gap-1.5 bg-white/80 backdrop-blur-sm p-3 rounded-xl border border-slate-100 text-[11px] font-medium text-slate-500 shadow-sm pointer-events-none">
        <div className="flex items-center gap-2">
          <span className="w-3.5 h-3.5 rounded bg-[#76b07a] border border-emerald-600/10"></span>
          <span>Has data</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="w-3.5 h-3.5 rounded bg-[#e2e8f0] border border-slate-300/10"></span>
          <span>No data</span>
        </div>
      </div>
    </div>
  );
};

export default IndiaMap;

