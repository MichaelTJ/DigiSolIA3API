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

function escapeXml(str) {
  return String(str ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function tagName(key) {
  return key.replace(/[^a-zA-Z0-9_-]/g, '_');
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
    const recordsXml = data.map(row =>
      '  <record>\n' +
      headers.map(h => `    <${tagName(h)}>${escapeXml(row[h])}</${tagName(h)}>`).join('\n') +
      '\n  </record>'
    ).join('\n');

    const xml = '<?xml version="1.0" encoding="UTF-8"?>\n' +
      '<sensors count="' + data.length + '">\n' +
      recordsXml + '\n' +
      '</sensors>';

    res.setHeader('Content-Type', 'application/xml; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="sensors.xml"');
    res.setHeader('Access-Control-Allow-Origin', '*');
    return res.status(200).send(xml);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
};
