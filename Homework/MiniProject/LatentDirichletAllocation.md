# Latent Dirichlet Allocation

[TOC]

## Background Knowledge

### Bag-of-Words Model

A text (such as a sentence or a document) is represented as the **bag** (multiset) of its **words**, disregarding grammar and even **word** order but keeping multiplicity.

### Binomial distribution

If $X \sim B(n,p)$, then $P(K=k)={N\choose k} p^k (1-p)^{n-k}$.

### Multinomial Distribution

Assuming the possible result for each experiment could be $(1,2,3,\ldots,k)$, then repeate $n$  
$$
P(x_1,x_2,\ldots,x_k; n, p_1,p_2,\ldots,p_k) = \frac{n!}{x_1 ! \ldots x_k !} p_1^k \cdots p_k^{x_k}
$$

### Gamma Function

Definition of Gamma function:
$$
\Gamma(x) = \int^{\infty}_0{t^{x-1} e^{-t} dt}.
$$
And the property of Gamma function:
$$
\Gamma(x+1) = x \Gamma(x).
$$
Gamma function could be treated as the extension of factorial on real numbers $\mathbb{R}$.

### Beta Distribution

For $\alpha, \beta >0$, and random variable $x\in[0,1]$:
$$
f(x;\alpha,\beta) = \frac{1}{B(\alpha,\beta)} x^{\alpha - 1} (1-x)^{\beta-1},
$$
Where 
$$
\frac{1}{B(\alpha,\beta)} =\frac{\Gamma(\alpha+\beta)}{\Gamma(\alpha)\Gamma(\beta)}.
$$

### Conjugate Prior Distribution

