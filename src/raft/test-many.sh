rm log*.log
for i in {0..10};do go test -race -run 2C > log$i.log;done