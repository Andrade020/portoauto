// Espera o documento HTML estar completamente carregado antes de executar o script
document.addEventListener('DOMContentLoaded', function() {

    // Pega o elemento body para ler os dados
    const body = document.body;
    // Lê os dados dos atributos data-* e converte de string JSON para objeto JavaScript
    const tableData = JSON.parse(body.dataset.tableData);
    const chartData = JSON.parse(body.dataset.chartData);

    // O resto do código é exatamente o mesmo de antes, usando as variáveis que acabamos de criar

    // Funções de formatação e inicialização da tabela
    const formatPercent = (data) => data !== null ? (data * 100).toFixed(2) + '%' : 'N/A';
    const formatCurrency = (data) => data !== null ? new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(data) : 'N/A';
    
    const table = new DataTable('#details-table', {
        data: tableData, // Usa a variável lida do data-*
        columns: [
            { data: 'Nome do Ente' }, 
            { data: 'Valor Líquido', render: formatCurrency }, 
            { data: '% Carteira', render: formatPercent },
            { data: '% Vencidos', render: formatPercent }, 
            { data: '% PDD', render: formatPercent },
            { data: 'TIR bruta a.m.', render: formatPercent },
            { data: 'TIR líquida a.m.', render: formatPercent },
            { data: '# Contratos' }
        ],
        responsive: true, 
        language: { url: '//cdn.datatables.net/plug-ins/2.0.8/i18n/pt-BR.json' }
    });

    const percentageTicks = (value) => `${(value * 100).toFixed(0)}%`;

    function handleChartClick(event, elements, chart) {
        if (elements.length > 0) {
            const index = elements[0].index;
            const ente = chart.data.labels[index];
            updateDashboard(ente);
        }
    }

    // --- RENDERIZAÇÃO DOS GRÁFICOS COM CHART.JS ---
    // 1. Gráfico de Composição (Pizza)
    new Chart(document.getElementById('compositionChart'), {
        type: 'pie',
        data: chartData.composition, // Usa a variável lida do data-*
        options: {
            responsive: true,
            plugins: { title: { display: true, text: 'Composição da Carteira (% Valor Líquido)' } },
            onClick: (event, elements) => handleChartClick(event, elements, event.chart)
        }
    });

    // 2. Gráfico de Risco (Barras)
    new Chart(document.getElementById('riskChart'), {
        type: 'bar',
        data: chartData.risk, // Usa a variável lida do data-*
        options: {
            responsive: true,
            plugins: { title: { display: true, text: 'Ranking de Risco (% Vencidos)' } },
            scales: { y: { ticks: { callback: percentageTicks } } }
        }
    });

    // 3. Gráfico de Rentabilidade (Barras)
    new Chart(document.getElementById('tirChart'), {
        type: 'bar',
        data: chartData.tir, // Usa a variável lida do data-*
        options: {
            responsive: true,
            plugins: { title: { display: true, text: 'Ranking de Rentabilidade (TIR Líquida a.m.)' } },
            scales: { y: { ticks: { callback: percentageTicks } } }
        }
    });

    // 4. Gráfico de Risco vs. Retorno (Dispersão)
    new Chart(document.getElementById('scatterChart'), {
        type: 'scatter',
        data: chartData.scatter, // Usa a variável lida do data-*
        options: {
            responsive: true,
            plugins: {
                title: { display: true, text: 'Risco (% Vencidos) vs. Retorno (TIR Líquida)' },
                tooltip: { callbacks: { label: (context) => context.raw.ente } }
            },
            scales: {
                x: { title: { display: true, text: 'Risco (% Vencidos)' }, ticks: { callback: percentageTicks } },
                y: { title: { display: true, text: 'Retorno (TIR Líquida a.m.)' }, ticks: { callback: percentageTicks } }
            }
        }
    });

    // Lógica de interatividade
    async function updateDashboard(ente = '* CARTEIRA *') {
        try {
            const response = await fetch(`/api/data?ente=${encodeURIComponent(ente)}`);
            if (!response.ok) throw new Error('Falha ao buscar dados');
            const data = await response.json();
            $('#kpi-title').text(`Resumo de: ${data.ente_selecionado}`);
            $('#kpi-valor-liquido').text(data.kpis.valor_liquido);
            $('#kpi-contratos').text(data.kpis.contratos);
            $('#kpi-inadimplencia').text(data.kpis.inadimplencia);
            $('#kpi-provisionamento').text(data.kpis.provisionamento);
            $('#kpi-rentabilidade').text(data.kpis.rentabilidade);
            table.clear().rows.add(data.table_data).draw();
        } catch (error) {
            console.error('Erro ao atualizar o dashboard:', error);
        }
    }
    $('#reset-filter-btn').on('click', () => updateDashboard('* CARTEIRA *'));
});