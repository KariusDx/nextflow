## Introduciton
We modified the nf-amazon to support run nextflow on aws batch with LakeFS backed storage.

## Quick start

Install/Upgrade to latest nextflow  
```bash
   curl -fsSL https://get.nextflow.io | bash
```

Download customized nf-amazon plugin
```bash
   aws s3 sync s3://misc.kariusdx.com/xfei/nf-amazon-2.1.4 ~/.nextflow/plugins/nf-amazon-2.1.4
```

Create a nextflow config file similar as the one in the example, and update with lakeFS access key and secrets (get from lasspass or ask Xubo)
 

## Examples

### Hello World Example
Download a file from lakeFS s3://demo/main/test.txt .  `demo` is a repo name and `main` is branch name.

Run with local docker.
```bash
   nextflow run s3.nf -profile local --demo=demo
```

Run with AWS batch.
```bash
   nextflow run s3.nf -profile remote -bucket-dir s3://demo/main/work --demo=demo
```


### Alignment Pipeline Example
A simple pipeline to take any Karius cfDNA dataset(s) and align to any reference genome in NCBI

Borrow from Nick's demo pipeline, see: https://github.com/KariusDx/nextflow-demo

Run with local docker not supported now due to download from regular s3 buckets is hard to support in local execution.


Run with AWS batch.
```bash
   nextflow run main.nf -profile remote -bucket-dir s3://demo/main/work --env prod --result 119159 --eukaryote GCA_002996065.1
```

