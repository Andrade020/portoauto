-- fold-output.lua (VERSÃO FINAL CORRIGIDA)

-- Esta função será executada uma vez para o documento inteiro
function Pandoc(doc)
    local script = [[
  <script>
  window.addEventListener('DOMContentLoaded', (event) => {
      window.toggleOutput = function(id) {
        var x = document.getElementById(id);
        if (x) {
          if (x.style.display === "none") {
            x.style.display = "block";
          } else {
            x.style.display = "none";
          }
        }
      }
  });
  </script>
    ]]
  
    if quarto.doc.is_format("html") then
      table.insert(doc.meta['header-includes'], pandoc.RawBlock('html', script))
    end
    return doc
  end
  
  -- Esta função modifica cada Div individualmente
  function Div(el)
    if quarto.doc.is_format("html") then
      if el.classes:includes("fold-output") then
        
        --------------------------------------------------
        -- BLOCO DE CORREÇÃO --
        -- Encontra o índice da classe "fold-output" e a remove pelo número
        local index_to_remove
        for i, class in ipairs(el.classes) do
          if class == "fold-output" then
            index_to_remove = i
            break
          end
        end
        if index_to_remove then
          table.remove(el.classes, index_to_remove)
        end
        --------------------------------------------------
  
        el.attributes['style'] = 'display: none;'
        local output_id = 'output-' .. (el.id or tostring(math.random(1, 10000)))
        el.attributes['id'] = output_id
  
        local button = pandoc.RawBlock('html',
          string.format('<p><button type="button" class="btn btn-sm btn-secondary" onclick="toggleOutput(\'%s\')">Mostrar/Ocultar Saída</button></p>', output_id)
        )
  
        return {button, el}
      end
    end
  end