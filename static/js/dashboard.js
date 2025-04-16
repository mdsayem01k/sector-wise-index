// Connect to Socket.IO server
const socket = io();

// Store current data
let currentData = [];
let sectors = new Set();

// Initialize charts
function initCharts() {
    // Create empty sector distribution chart
    Plotly.newPlot('sectorChart', [{
        type: 'bar',
        x: [],
        y: [],
        marker: {
            color: 'rgba(13, 110, 253, 0.7)'
        }
    }], {
        margin: { t: 10, b: 40, l: 40, r: 10 },
        xaxis: { title: 'Sector Code' },
        yaxis: { title: 'Number of Companies' }
    });
}

// Update sector distribution chart
function updateSectorChart(data) {
    // Count companies per sector
    const sectorCounts = {};
    data.forEach(item => {
        if (sectorCounts[item.sector_code]) {
            sectorCounts[item.sector_code]++;
        } else {
            sectorCounts[item.sector_code] = 1;
        }
    });
    
    // Prepare data for chart
    const sectors = Object.keys(sectorCounts);
    const counts = sectors.map(sector => sectorCounts[sector]);
    
    // Update chart
    Plotly.react('sectorChart', [{
        type: 'bar',
        x: sectors,
        y: counts,
        marker: {
            color: 'rgba(13, 110, 253, 0.7)'
        }
    }], {
        margin: { t: 10, b: 40, l: 40, r: 10 },
        xaxis: { title: 'Sector Code' },
        yaxis: { title: 'Number of Companies' }
    });
}

// Update table with data
function updateTable(data) {
    const tableBody = document.getElementById('tableBody');
    const currentRows = new Set();
    
    // Clear sectors set and rebuild
    sectors = new Set();
    
    // Filter data based on current filters
    const sectorFilter = document.getElementById('sectorFilter').value;
    const companySearch = document.getElementById('companySearch').value.toLowerCase();
    
    const filteredData = data.filter(item => {
        // Add to sectors set
        sectors.add(item.sector_code);
        
        // Apply filters
        const sectorMatch = sectorFilter === 'all' || item.sector_code === sectorFilter;
        const companyMatch = !companySearch || item.company.toLowerCase().includes(companySearch);
        
        return sectorMatch && companyMatch;
    });
    
    // Update sector filter options if needed
    updateSectorFilter(sectors);
    
    // Clear table
    tableBody.innerHTML = '';
    
    // Add rows
    filteredData.forEach(item => {
        const row = tableBody.insertRow();
        
        // Format date for display
        const lastUpdated = new Date(item.last_updated).toLocaleString();
        
        // Add cells
        const companyCell = row.insertCell(0);
        const sectorCell = row.insertCell(1);
        const updatedCell = row.insertCell(2);
        
        companyCell.textContent = item.company;
        sectorCell.textContent = item.sector_code;
        updatedCell.textContent = lastUpdated;
        
        // Add class for new or updated data
        if (!currentRows.has(item.company)) {
            row.classList.add('updated-row');
        }
        
        currentRows.add(item.company);
    });
    
    // Update statistics
    document.getElementById('totalCompanies').textContent = data.length;
    document.getElementById('totalSectors').textContent = sectors.size;
    document.getElementById('lastUpdate').textContent = new Date().toLocaleString();
}

// Update sector filter options
function updateSectorFilter(sectors) {
    const sectorFilter = document.getElementById('sectorFilter');
    const currentValue = sectorFilter.value;
    
    // Save current options
    const currentOptions = new Set([...sectorFilter.options].map(opt => opt.value));
    
    // Add new options
    sectors.forEach(sector => {
        if (!currentOptions.has(sector)) {
            const option = document.createElement('option');
            option.value = sector;
            option.textContent = sector;
            sectorFilter.appendChild(option);
        }
    });
}

// Initialize page
document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    
    // Set up event listeners
    document.getElementById('sectorFilter').addEventListener('change', () => {
        updateTable(currentData);
        updateSectorChart(currentData);
    });
    
    document.getElementById('companySearch').addEventListener('input', () => {
        updateTable(currentData);
    });
    
    // Socket.IO event listeners
    socket.on('connect', () => {
        console.log('Connected to server');
    });
    
    socket.on('data_update', (data) => {
        console.log('Received data update', data);
        currentData = data;
        updateTable(data);
        updateSectorChart(data);
    });
    
    socket.on('disconnect', () => {
        console.log('Disconnected from server');
    });
});