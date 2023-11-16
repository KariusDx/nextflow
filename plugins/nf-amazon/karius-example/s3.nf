process foo {
  debug true
  label 'cloud'
  input:
  path(obj)

  """
  cat $obj | head
  """
}


workflow {
  def s3file = file("s3://${params.demo}/main/test.txt")

  foo(s3file)
}
