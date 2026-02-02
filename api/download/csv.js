const { parse } = require('csv-parse/sync');
const { readFileSync } = require('fs');
const { join } = require('path');

let cachedData = null;

function loadData() {
  if (cachedData) return cachedData;
  const csvPath = join(__dirname, '..', 'data.csv');
  const fileContent = readFileSync(csvPath, 'utf-8');
  const records = parse(fileContent, {
    columns: true,
    skip_empty_lines: true,
  });
  cachedData = records;
  return records;
}

function applyFilters(data, query) {
  const { asset_id, status, traffic, delay_reason, hash } = query;
  if (asset_id) {
    data = data.filter(row => row.Asset_ID.toLowerCase() === asset_id.toLowerCase());
  }
  if (status) {
    data = data.filter(row => row.Shipment_Status.toLowerCase() === status.toLowerCase());
  }
  if (traffic) {
    data = data.filter(row => row.Traffic_Status.toLowerCase() === traffic.toLowerCase());
  }
  if (delay_reason) {
    data = data.filter(row => row.Logistics_Delay_Reason.toLowerCase() === delay_reason.toLowerCase());
  }
  if (hash) {
    data = data.filter(row => row.All_Data_Hash === hash);
  }
  return data;
}

function escapeCsvField(value) {
  const s = String(value ?? '');
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

module.exports = function handler(req, res) {
  try {
    let data = applyFilters(loadData(), req.query);
    const headers = data.length ? Object.keys(data[0]) : [
      'Timestamp', 'Asset_ID', 'Latitude', 'Longitude', 'Inventory_Level', 'Shipment_Status',
      'Temperature', 'Humidity', 'Traffic_Status', 'Waiting_Time', 'User_Transaction_Amount',
      'User_Purchase_Frequency', 'Logistics_Delay_Reason', 'Asset_Utilization', 'Demand_Forecast',
      'Logistics_Delay', 'All_Data_Hash', 'Location_Status_Hash', 'Env_Time_Hash'
    ];
    const headerLine = headers.map(escapeCsvField).join(',');
    const rows = data.map(row => headers.map(h => escapeCsvField(row[h])).join(','));
    const csv = [headerLine, ...rows].join('\n');

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="sensors.csv"');
    res.setHeader('Access-Control-Allow-Origin', '*');
    return res.status(200).send(csv);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
};
