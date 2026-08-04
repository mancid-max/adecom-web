(function () {
  var btn = document.getElementById('printTopArticulos');
  if (!btn) return;

  btn.addEventListener('click', function () {
    var todos = window._topArticulosAll;

    var tableRows = '';
    if (Array.isArray(todos) && todos.length) {
      todos.forEach(function (item, i) {
        tableRows +=
          '<tr>' +
          '<td>' + (i + 1) + '</td>' +
          '<td>' + (item.articulo || '') + '</td>' +
          '<td>' + (item.total || '') + '</td>' +
          '</tr>';
      });
    } else {
      var tbody = document.getElementById('topArticulosBody');
      if (tbody) {
        tbody.querySelectorAll('tr').forEach(function (tr) {
          var cells = tr.querySelectorAll('td');
          if (cells.length < 3) return;
          tableRows +=
            '<tr>' +
            '<td>' + (cells[0].textContent || '') + '</td>' +
            '<td>' + (cells[1].textContent || '') + '</td>' +
            '<td>' + (cells[2].textContent || '') + '</td>' +
            '</tr>';
        });
      }
    }

    var win = window.open('', '_blank');
    win.document.write(
      '<!DOCTYPE html>' +
      '<html><head><meta charset="utf-8">' +
      '<title>Top Artículos con más Pedidos</title>' +
      '<style>' +
      'body{font-family:Arial,sans-serif;font-size:13px;margin:20px}' +
      'h2{margin-bottom:12px}' +
      'table{border-collapse:collapse;width:100%}' +
      'th,td{border:1px solid #ccc;padding:5px 10px;text-align:left}' +
      'th{background:#f0f0f0}' +
      '@media print{@page{margin:1cm}}' +
      '</style>' +
      '</head><body>' +
      '<h2>Top Artículos con más Pedidos</h2>' +
      '<table>' +
      '<thead><tr><th>#</th><th>Artículo</th><th>Pedidos</th></tr></thead>' +
      '<tbody>' + tableRows + '</tbody>' +
      '</table>' +
      '</body></html>'
    );
    win.document.close();
    win.focus();
    win.print();
  });
})();
