import { parse } from 'csv-parse/sync';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let cachedData = null;

function loadData() {
  if (cachedData) return cachedData;
  
  const csvPath = join(__dirname, '..', 'smart_logistics_dataset_transformed.csv');
  const fileContent = readFileSync(csvPath, 'utf-8');
  
  const records = parse(fileContent, {
    columns: true,
    skip_empty_lines: true,
    cast: (value, context) => {
      if (context.header) return value;
      // Cast numeric fields
      const numericFields = [
        'Latitude', 'Longitude', 'Inventory_Level', 'Temperature', 
        'Humidity', 'Waiting_Time', 'User_Transaction_Amount', 
        'User_Purchase_Frequency', 'Asset_Utilization', 
        'Demand_Forecast', 'Logistics_Delay'
      ];
      if (numericFields.includes(context.column)) {
        return parseFloat(value);
      }
      return value;
    }
  });
  
  cachedData = records;
  return records;
}

export default function handler(req, res) {
  try {
    let data = loadData();
    const { 
      asset_id, 
      status, 
      traffic,
      delay_reason,
      hash,
      limit = 100, 
      offset = 0 
    } = req.query;

    // Filter by Asset_ID
    if (asset_id) {
      data = data.filter(row => row.Asset_ID.toLowerCase() === asset_id.toLowerCase());
    }

    // Filter by Shipment_Status
    if (status) {
      data = data.filter(row => row.Shipment_Status.toLowerCase() === status.toLowerCase());
    }

    // Filter by Traffic_Status
    if (traffic) {
      data = data.filter(row => row.Traffic_Status.toLowerCase() === traffic.toLowerCase());
    }

    // Filter by Logistics_Delay_Reason
    if (delay_reason) {
      data = data.filter(row => row.Logistics_Delay_Reason.toLowerCase() === delay_reason.toLowerCase());
    }

    // Filter by SHA256_Hash (exact match)
    if (hash) {
      data = data.filter(row => row.SHA256_Hash === hash);
    }

    const total = data.length;
    const paginatedData = data.slice(Number(offset), Number(offset) + Number(limit));

    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Cache-Control', 's-maxage=60, stale-while-revalidate');
    
    return res.status(200).json({
      total,
      limit: Number(limit),
      offset: Number(offset),
      data: paginatedData
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}
