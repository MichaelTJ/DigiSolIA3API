import { parse } from 'csv-parse/sync';
import { readFileSync } from 'fs';
import { join } from 'path';

let cachedData = null;

function loadData() {
  if (cachedData) return cachedData;
  
  const csvPath = join(process.cwd(), 'smart_logistics_dataset_transformed.csv');
  const fileContent = readFileSync(csvPath, 'utf-8');
  
  const records = parse(fileContent, {
    columns: true,
    skip_empty_lines: true,
    cast: (value, context) => {
      if (context.header) return value;
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
    const { hash } = req.query;
    const data = loadData();
    
    const record = data.find(row => row.SHA256_Hash === hash);
    
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Cache-Control', 's-maxage=60, stale-while-revalidate');
    
    if (!record) {
      return res.status(404).json({ error: 'Record not found' });
    }
    
    return res.status(200).json(record);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}
